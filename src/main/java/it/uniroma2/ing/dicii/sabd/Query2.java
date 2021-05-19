package it.uniroma2.ing.dicii.sabd;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;
import scala.math.Ordering;
import scala.reflect.ClassTag;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class Query2 {

    private final static String FIRSTFEB2021 = "2021-02-01";

    /*  Funzione temporanea per la scrittura dei risultati su file */
    private static void writeResultToCSV(String result, String path){
        try {
            FileWriter fileWriter = new FileWriter(path);
            fileWriter.write(result);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ParseException {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat simpleMonthFormat = new SimpleDateFormat("MM");
        Date firstFeb2021 = simpleDateFormat.parse(FIRSTFEB2021);

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
           String age = line.getString(3);              //convertire subito a data
           Double vaccinations = (double)line.getLong(5);

           return new Tuple2<>(new Tuple3<>(date,age,region), vaccinations);
        });

        /*  Ottengo i valori a partire dallo 2021-02-01 */

        dateRegionAgeVaccinations = dateRegionAgeVaccinations.filter(record -> {
            Date date = record._1()._1();
            return !(date.before(firstFeb2021));
        });

        /*  Ottengono l'unicità della chiave aggregando i valori con la stessa chiave
        * Questi valori esistono a causa delle diverse marche di vaccini.*/
        dateRegionAgeVaccinations = dateRegionAgeVaccinations.reduceByKey((a,b) -> a+b);

        /*  Ottengo [(mese, regione, età),(data, vaccinazioni)]
        * Le chiavi sono duplicate - i record con chiave duplicata differiscono sul valore data */
        JavaPairRDD<Tuple3<String, String, String>, Tuple2<Date,Double>> monthRegionAgeDateVaccinations = dateRegionAgeVaccinations.mapToPair(
                record -> {
                    Date date = record._1()._1();
                    String month = simpleMonthFormat.format(date);
                    String age = record._1()._2();
                    String region = record._1()._3();
                    Double vaccinations = record._2();
                    return new Tuple2<>(new Tuple3<>(month, age, region), new Tuple2<>(date,vaccinations));
                }
        );

        /*  Ottengo [(mese, regione, età),[(data, vaccinazioni)]]
        * La chiave è univoca - le date nella lista dei valori si riferiscono al mese nella chiave */
        JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<Date,Double>>> monthRegionAgeDateVaccinationsIterable = monthRegionAgeDateVaccinations.groupByKey();

        /* Considero solo le fasce d'età per cui nel mese in questione sono registrate almeno 2 giorni di vaccinazioni*/
        monthRegionAgeDateVaccinationsIterable = monthRegionAgeDateVaccinationsIterable.filter(record -> {
            Iterable<Tuple2<Date,Double>> dateVaccinationsList = record._2();
            int count = 0;
            for(Tuple2<Date,Double> entry:dateVaccinationsList){
                count = count + 1;
                if(count == 2){
                    return true;
                }
            }
            return false;
        });


        /*  Effettuo regressione lineare associando ad ogni (mese, regione, fascia) il valore predetto al primo
         * giorno del mese successivo.
         * Effettuo caching del risultato in quando dovrò andare ad applicare iterativamente filtri su di esso.
         * NOTA PER LA RELAZIONE:
         * Il parallelismo si ottiene perché valori con chiave diversa dovrebbero poter appartenere a partizioni diverse
         * e dunque poter essere processate in parallelo.*/
        JavaPairRDD<Double, Tuple3<String, String, String>> dateAgeRegionPredictedVaccinations = monthRegionAgeDateVaccinationsIterable.mapToPair(
                record -> {
                    int month = Integer.parseInt(record._1()._1());
                    int nextMonth = month % 12 + 1;
                    String firstDayNextMonth = null;
                    if(nextMonth<10){
                        firstDayNextMonth = "2021-0" + nextMonth + "-01";
                    } else {
                        firstDayNextMonth = "2021-" + nextMonth + "-01";
                    }

                    Iterable<Tuple2<Date, Double>> dateVaccinations = record._2();
                    SimpleRegression simpleRegression = new SimpleRegression();
                    for (Tuple2<Date, Double> elem : dateVaccinations) {
                        Date date = elem._1();
                        double dateFromEpoch = (double) date.getTime();
                        double vaccinations = elem._2();

                        simpleRegression.addData(dateFromEpoch, vaccinations);
                    }
                    double xToBePredicted = (double) simpleDateFormat.parse(firstDayNextMonth).getTime();
                    double yPredicted = simpleRegression.predict(xToBePredicted);

                    String age = record._1()._2();
                    String region = record._1()._3();
                    return new Tuple2<>(yPredicted, new Tuple3<>(firstDayNextMonth, age, region));
                }
        ).cache();

        /* Ottengo [(primo giorno prossimo mese, età)]
         * Le ripetizioni dovute al fatto che esiste una coppia per ogni regione sono eliminate. */
        JavaRDD<Tuple2<String,String>> dateAgeKeys = dateAgeRegionPredictedVaccinations.map(
                record -> {
                    String date = record._2()._1();
                    String age = record._2()._2();
                    return new Tuple2<>(date, age);
                }
        ).distinct();
        /* Ordino la lista [(primo giorno prossimo mese, età)]*/
        List<Tuple2<String,String>> dateAgeList = dateAgeKeys.sortBy(new Function<Tuple2<String, String>, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Tuple2<String, String> value) throws Exception{
                return value._1()+value._2();
            }
        }, true, 1).collect();

        /*Il valore result mi serve per scrivere l'output
         * valutare gli aspetti di scrittura dell'output ed eventualmente eliminarlo.
         */
        String result = "";

        /* Per ogni (primo giorno del mese successivo, età) */
        for(Tuple2<String,String> key: dateAgeList) {

            /* Seleziono i valori che si riferiscono a quel giorno e quella fascia anagrafica */
            JavaPairRDD<Double, Tuple3<String, String, String>> tempdateAgeRegionPredictedVaccinations = dateAgeRegionPredictedVaccinations.filter(
                    record -> {
                        String dateRecord = record._2()._1();
                        String ageRecord = record._2()._2();
                        String dateKey = key._1();
                        String ageKey = key._2();
                        return dateRecord.equalsIgnoreCase(dateKey) && ageRecord.equals(ageKey);
                    }
            );
            /* Ottengo la top5 dei valori che si riferiscono a quel giorno e quella fascia anagrafica*/
            List<Tuple2<Double, Tuple3<String, String, String>>> tempTop5 = tempdateAgeRegionPredictedVaccinations.sortByKey(false).take(5);

            /*  Ciclo per la scrittura dell'output
             * valutare gli aspetti di scrittura dell'output ed eventualmente eliminarlo.
             * */
            for(Tuple2<Double, Tuple3<String, String, String>> elem: tempTop5){
                String date = elem._2()._1();
                String age = elem._2()._2();
                String region = elem._2()._3();
                int vaccinations = elem._1().intValue();
                result = result + date + "," + age + "," + region + "," + vaccinations + "\n";
            }

        }

        JavaRDD<String> rddResult = sparkContext.parallelize(Arrays.asList(result.split("\n")));
        rddResult.saveAsTextFile("hdfs://hdfs-master:54310" + "/output/query2Result");

        Instant end = Instant.now();
        log.info("Query completed in " + Duration.between(start, end).toMillis() + "ms");


        log.info("Result:");
        String[] lines = result.split("\n");
        for(String line: lines){
            log.info(line);
        }


        spark.close();
    }

}
