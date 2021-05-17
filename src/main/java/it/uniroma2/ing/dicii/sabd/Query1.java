package it.uniroma2.ing.dicii.sabd;

import it.uniroma2.ing.dicii.sabd.utils.ClusterConfig;
import it.uniroma2.ing.dicii.sabd.utils.ValuesComparator;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.json.simple.parser.ParseException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.List;


public class Query1 {
    public static void main( String[] args ) {
        Logger log = LogManager.getLogger("SABD-PROJECT");
        ValuesComparator<Date, String> valuesComparator = new ValuesComparator<>(Comparator.<Date>naturalOrder(), Comparator.<String>naturalOrder());

        /*ClusterConfig clusterConfig = null;
        try {
            clusterConfig = ClusterConfig.ParseConfig("./config.json");
        } catch (MissingArgumentException | ParseException | IOException e) {
            log.error("ERRRRRRRRROEEEOEOEOEOOE");
            log.error(e.getMessage());
            System.exit(1);
        }*/
        /*
        SparkConf conf = new SparkConf()
                .setMaster(clusterConfig.getSparkURL())
                .setAppName("Query 1");
        JavaSparkContext sc = new JavaSparkContext(conf); */

        SparkSession spark = SparkSession
                .builder()
                .appName("Query1")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        log.info("Starting processing query");

        Instant start = Instant.now();
        //JavaRDD<String> rawSummary = sc.textFile(clusterConfig.getHdfsURL()
        //        + "/input/somministrazioni-vaccini-summary-latest.csv");
        Dataset<Row> datasetSummary = spark.read().parquet("hdfs://hdfs-master:54310"
                + "/input/somministrazioni-vaccini-summary-latest.parquet");

        JavaRDD<Row> rawSummary = datasetSummary.toJavaRDD();

        JavaPairRDD<Date, Tuple2<String, Long>> parsedSummary = rawSummary.mapToPair((row -> {
            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(row.getString(0));
            return new Tuple2<>(date, new Tuple2<>(row.getString(1), row.getLong(2)));
        })).sortByKey(true).cache();

        JavaPairRDD<Tuple2<Date, String>, Long> vaccinePerMonthArea = parsedSummary.mapToPair((line) -> {

            Calendar cal = Calendar.getInstance();
            cal.setTime(line._1);
            cal.set(Calendar.DAY_OF_MONTH, 1);
            return new Tuple2<>(new Tuple2<>(cal.getTime(), line._2._1), line._2._2);
        }).reduceByKey(Long::sum);

        JavaPairRDD<String, Tuple2<Date, Long>> vaccinePerArea = vaccinePerMonthArea.mapToPair((line) ->
                new Tuple2<>(line._1._2, new Tuple2<>(line._1._1, line._2)));

        Dataset<Row> datasetCenters = spark.read().parquet("hdfs://hdfs-master:54310"
                + "/input/punti-somministrazione-tipologia.parquet");

        JavaPairRDD<String, Integer> vaccineCenters = datasetCenters.toJavaRDD().mapToPair((row) ->
                new Tuple2<> (row.getString(0), 1)).reduceByKey(Integer::sum);

        JavaPairRDD<String, Tuple2<Tuple2<Date, Long>, Integer>> vaccinePerAreaMonthCenters =
                vaccinePerArea.join(vaccineCenters);

        JavaPairRDD<Tuple2<Date, String>, Long> result = vaccinePerAreaMonthCenters.mapToPair((line) ->
                new Tuple2<>(new Tuple2<>(line._2._1._1, line._1), line._2._1._2 / line._2._2)).sortByKey(valuesComparator);

        result.saveAsTextFile("hdfs://hdfs-master:54310" + "/output/query1Result");
        Instant end = Instant.now();

        log.info("Query completed in " + Duration.between(start, end).toMillis() + "ms");

        List<Tuple2<Tuple2<Date, String>, Long>> values = result.collect();
        log.info("Result:");
        for (Tuple2<Tuple2<Date, String>, Long> value: values) {
            log.info(value);
        }

        spark.close();
    }
}
