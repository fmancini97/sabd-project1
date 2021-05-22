package it.uniroma2.ing.dicii.sabd;

import it.uniroma2.ing.dicii.sabd.utils.Tuple3Comparator;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;


public class Query2 {

    public static void main(String[] args)  {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat simpleMonthFormat = new SimpleDateFormat("MM");
        SimpleDateFormat simpleItalianDateFormat = new SimpleDateFormat("dd-MMMM", Locale.ITALIAN);
        Date dateFirstFeb2021 = new GregorianCalendar(2021, Calendar.FEBRUARY, 1).getTime();
        List<StructField> resultfields = new ArrayList<>();
        resultfields.add(DataTypes.createStructField("data", DataTypes.StringType, false));
        resultfields.add(DataTypes.createStructField("fascia anagrafica", DataTypes.StringType, false));
        resultfields.add(DataTypes.createStructField("regione", DataTypes.StringType, false));
        resultfields.add(DataTypes.createStructField("vaccinazioni previste", DataTypes.IntegerType, false));
        StructType resultStruct = DataTypes.createStructType(resultfields);

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
                + "/sabd/input/somministrazioni-vaccini-latest.parquet");

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


        JavaPairRDD<Tuple3<String, String, String>, Integer> counts =
                (monthRegionAgeDateVaccinations.keys().mapToPair(k -> new Tuple2<>(k,1)))
                        .reduceByKey(Integer::sum);

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


        JavaPairRDD<Tuple3<Date, String, Double>, String> dateAgePredictedVaccinationsRegion =
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
                    Tuple3<Date,String,Double> newKey = new Tuple3<>(dateFirstDayNextMonth, record._1._2(),predictedVaccinations);
                    String region = record._1._3();
                    return new Tuple2<>(newKey, region);
                });

        Tuple3Comparator<Date,String,Double> tuple3Comparator = new Tuple3Comparator<>(Comparator.<Date>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<Double>naturalOrder());
        dateAgePredictedVaccinationsRegion = dateAgePredictedVaccinationsRegion.sortByKey(tuple3Comparator,false,1).cache();

        List<Tuple2<Date,String>> dateAgeKeys = dateAgePredictedVaccinationsRegion.map(
                record -> {
                    Date date = record._1._1();
                    String age = record._1._2();
                    return new Tuple2<>(date, age);
                }
        ).distinct().collect();


        JavaPairRDD<Tuple2<Tuple3<Date, String, Double>, String>, Long> resultsRDD = null;
        for(Tuple2<Date,String> key: dateAgeKeys) {
            JavaPairRDD<Tuple3<Date, String, Double>, String> filteredData = dateAgePredictedVaccinationsRegion.filter(record -> record._1._1().equals(key._1)&&record._1._2().equals(key._2));

            JavaPairRDD<Tuple2<Tuple3<Date, String, Double>, String>, Long> zippedData = filteredData.zipWithIndex();
            log.info("Valori chiave: " + key.toString());
            for(Tuple2<Tuple2<Tuple3<Date, String, Double>, String>, Long> elem: zippedData.collect()){
                log.info(elem);
            }
            JavaPairRDD<Tuple2<Tuple3<Date, String, Double>, String>, Long> top5 = zippedData.filter(record -> record._2<5);
            if(resultsRDD == null)
                resultsRDD = top5;
            else
                resultsRDD = resultsRDD.union(top5);
        }
        
        assert resultsRDD != null;
        JavaRDD<Row> resultRow = resultsRDD.map(record ->
            RowFactory.create(simpleItalianDateFormat.format(record._1._1._1()),
                    record._1._1._2(), record._1._2, record._1._1._3().intValue()));

        Dataset<Row> clusterDF = spark.createDataFrame(resultRow, resultStruct);
        clusterDF.write()
                .format("csv")
                .option("header", true)
                .mode(SaveMode.Overwrite)
                .save("hdfs://hdfs-master:54310"
                        + "/sabd/output/query2Result");

        /*
        JavaRDD<String> resultRDD = result.mapToPair(record -> record._1)
                .repartitionAndSortWithinPartitions(new Partitioner() {
            @Override
            public int numPartitions() {
                return 1;
            }

            @Override
            public int getPartition(Object key) {
                return 0;
            }
        }, tuple3Comparator)/*
*/
        Instant end = Instant.now();
        log.info("Query completed in " + Duration.between(start, end).toMillis() + "ms");

        spark.close();

    }

}
