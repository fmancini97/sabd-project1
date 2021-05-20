package it.uniroma2.ing.dicii.sabd;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.Month;
import java.time.format.TextStyle;
import java.util.*;


public class Query2 {

    public static void main(String[] args)  {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat simpleMonthFormat = new SimpleDateFormat("MM");
        Calendar firstFebCal = Calendar.getInstance();
        firstFebCal.set(Calendar.YEAR, 2021);
        firstFebCal.set(Calendar.MONTH, Calendar.FEBRUARY);
        firstFebCal.set(Calendar.DAY_OF_MONTH, 1);
        Date dateFirstFeb2021 = firstFebCal.getTime();

        Logger log = LogManager.getLogger("SABD-PROJECT");

        SparkSession spark = SparkSession
                .builder()
                .appName("Query2")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        log.info("Starting processing query");
        Instant start = Instant.now();


        Dataset<Row> datasetSummary = spark.read().parquet("hdfs://hdfs-master:54310"
                + "/input/somministrazioni-vaccini-latest.parquet");

        JavaRDD<Row> rawSummary = datasetSummary.toJavaRDD();

        /*  Ottengo [(data, regione, età), vaccini]*/
        JavaPairRDD<Tuple3<Date, String, String>, Double> dateRegionAgeVaccinations = rawSummary.mapToPair(line ->{
           Date date = simpleDateFormat.parse(line.getString(0));
           String region = line.getString(21);
           String age = line.getString(3);
           Double vaccinations = (double)line.getLong(5);
           return new Tuple2<>(new Tuple3<>(date,age,region), vaccinations);
        });


        /*  Ottengo i valori a partire dallo 2021-02-01 */
        dateRegionAgeVaccinations = dateRegionAgeVaccinations.filter(record -> {
            Date date = record._1._1();
            return !(date.before(dateFirstFeb2021));
        });


        /*  Ottengono l'unicità della chiave aggregando i valori con la stessa chiave
        * Questi valori esistono a causa delle diverse marche di vaccini.*/
        dateRegionAgeVaccinations = dateRegionAgeVaccinations.reduceByKey(Double::sum);


        /*  Ottengo [(mese, regione, età),(data, vaccinazioni)]
        * Le chiavi sono duplicate - i record con chiave duplicata differiscono sul valore data */
        JavaPairRDD<Tuple3<String, String, String>, Tuple2<Date,Double>> monthRegionAgeDateVaccinations =
                dateRegionAgeVaccinations.mapToPair(
                record -> {
                    Date date = record._1._1();
                    String month = simpleMonthFormat.format(date);
                    String age = record._1._2();
                    String region = record._1._3();
                    Double vaccinations = record._2;
                    return new Tuple2<>(new Tuple3<>(month, age, region), new Tuple2<>(date,vaccinations));
                }
        );


        /* Per eliminare le chiavi non duplicate, cioè che hanno un solo valore associato
        * vedere il seguente sito per altre soluzioni
        * https://stackoverflow.com/questions/33161499/spark-rdd-remove-records-with-multiple-keys */
        JavaPairRDD<Tuple3<String, String, String>, Integer> counts =
                (monthRegionAgeDateVaccinations.keys().mapToPair(k -> new Tuple2<>(k,1)))
                        .reduceByKey(Integer::sum);

        /*
        * JavaPairRDD<Tuple3<String, String, String>, Integer> invalidEntries = counts.filter(record -> record._2<2);
        * monthRegionAgeDateVaccinations = monthRegionAgeDateVaccinations.subtractByKey(invalidEntries); */
        Broadcast<List<Tuple3<String, String, String>>> notDuplicated = sparkContext.broadcast(counts.filter(record -> record._2 == 1).keys().collect());
        monthRegionAgeDateVaccinations = monthRegionAgeDateVaccinations.filter(record -> !notDuplicated.value().contains(record._1));


        JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> monthRegionAgeRegressor =
                monthRegionAgeDateVaccinations.mapToPair(
                        line -> {
                            SimpleRegression simpleRegression = new SimpleRegression();
                            simpleRegression.addData((double) (line._2._1.getTime()), line._2._2);
                            return new Tuple2<>(line._1, simpleRegression);
                        });

        monthRegionAgeRegressor = monthRegionAgeRegressor.reduceByKey((a,b) -> {
            a.append(b);
            return a;
        });






        /*
        JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> monthRegionAgeRegressor =
                monthRegionAgeDateVaccinations.combineByKey(
                        record -> {
                            Date date = record._1;
                            Double vaccinations = record._2;
                            SimpleRegression simpleRegression = new SimpleRegression();
                            simpleRegression.addData((double)date.getTime(), vaccinations);
                            return simpleRegression;
                        },
                        (simpleRegression, record2) -> {
                            Date date = record2._1;
                            Double vaccinations = record2._2;
                            simpleRegression.addData((double)date.getTime(), vaccinations);
                            return simpleRegression;
                        },
                        (record1, record2) -> {
                            record1.append(record2);
                            return record1;
                        }
                );
        */

        JavaPairRDD<Double, Tuple3<String, String, String>> dateAgeRegionPredictedVaccinations =
                monthRegionAgeRegressor.mapToPair(record ->{
                    int month = Integer.parseInt(record._1._1());
                    int nextMonth = month % 12 + 1;
                    String stringFirstDayNextMonth;
                    if(nextMonth<10){
                        stringFirstDayNextMonth = "2021-0" + nextMonth + "-01";
                    } else {
                        stringFirstDayNextMonth = "2021-" + nextMonth + "-01";
                    }
                    Date dateFirstDayNextMonth = simpleDateFormat.parse(stringFirstDayNextMonth);
                    double predictedVaccinations = record._2.predict((double)(dateFirstDayNextMonth).getTime());
                    Tuple3<String,String,String> newKey = new Tuple3<>(stringFirstDayNextMonth, record._1._2(),record._1._3());
                    return new Tuple2<>(predictedVaccinations, newKey);
                }).cache();


        /* Ottengo [(primo giorno prossimo mese, età)]
         * Le ripetizioni dovute al fatto che esiste una coppia per ogni regione sono eliminate. */
        JavaRDD<Tuple2<String,String>> dateAgeKeys = dateAgeRegionPredictedVaccinations.map(
                record -> {
                    String date = record._2._1();
                    String age = record._2._2();
                    return new Tuple2<>(date, age);
                }
        ).distinct();


        /* Ordino la lista [(primo giorno prossimo mese, età)]*/
        List<Tuple2<String,String>> dateAgeList = dateAgeKeys.sortBy(new Function<Tuple2<String, String>, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Tuple2<String, String> value) {
                return value._1+value._2;
            }
        }, true, 1).collect();


        /*Il valore result mi serve per scrivere l'output
         * valutare gli aspetti di scrittura dell'output ed eventualmente eliminarlo.
         */
        StringBuilder result = new StringBuilder();
        /* Per ogni (primo giorno del mese successivo, età) */
        for(Tuple2<String,String> key: dateAgeList) {

            /* Seleziono i valori che si riferiscono a quel giorno e quella fascia anagrafica */
            JavaPairRDD<Double, Tuple3<String, String, String>> tempdateAgeRegionPredictedVaccinations = dateAgeRegionPredictedVaccinations.filter(
                    record -> {
                        String dateRecord = record._2._1();
                        String ageRecord = record._2._2();
                        String dateKey = key._1;
                        String ageKey = key._2;
                        return dateRecord.equalsIgnoreCase(dateKey) && ageRecord.equals(ageKey);
                    }
            );
            /* Ottengo la top5 dei valori che si riferiscono a quel giorno e quella fascia anagrafica*/
            List<Tuple2<Double, Tuple3<String, String, String>>> tempTop5 = tempdateAgeRegionPredictedVaccinations.sortByKey(false).take(5);

            /*  Ciclo per la scrittura dell'output
             * valutare gli aspetti di scrittura dell'output ed eventualmente eliminarlo.
             * */
            for(Tuple2<Double, Tuple3<String, String, String>> elem: tempTop5){
                String date = elem._2._1();
                String monthAsString = Month.of(Integer.parseInt(date.split("-")[1]))
                        .getDisplayName(TextStyle.FULL_STANDALONE, Locale.ITALIAN);
                date = "1 " + monthAsString + " 2021";
                String age = elem._2._2();
                String region = elem._2._3();
                int vaccinations = elem._1.intValue();
                result.append(date).append(",").append(age).append(",").append(region).append(",").append(vaccinations).append("\n");
            }

        }
        dateAgeRegionPredictedVaccinations.unpersist();

        JavaRDD<String> rddResult = sparkContext.parallelize(Arrays.asList(result.toString().split("\n")));
        rddResult.saveAsTextFile("hdfs://hdfs-master:54310" + "/output/query2Result");

        Instant end = Instant.now();
        log.info("Query completed in " + Duration.between(start, end).toMillis() + "ms");

        log.info("Result:");
        String[] lines = result.toString().split("\n");
        for(String line: lines){
            log.info(line);
        }

        spark.close();
    }

}
