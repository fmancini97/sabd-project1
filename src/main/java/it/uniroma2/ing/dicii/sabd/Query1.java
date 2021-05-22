package it.uniroma2.ing.dicii.sabd;

import it.uniroma2.ing.dicii.sabd.utils.Tuple2Comparator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Query1 {
    public static void main( String[] args ) {


        Logger log = LogManager.getLogger("SABD-PROJECT");
        Tuple2Comparator<Date, String> valuesComparator = new Tuple2Comparator<>(Comparator.<Date>naturalOrder(), Comparator.<String>naturalOrder());
        Date firstJanuary = new GregorianCalendar(2021, Calendar.JANUARY, 1).getTime();
        SimpleDateFormat outputFormat = new SimpleDateFormat("MMMM yyyy", Locale.ITALIAN);

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("data", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("regione", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("vaccinazioni per centro", DataTypes.LongType, false));
        StructType resultStruct = DataTypes.createStructType(fields);

        /*
        ClusterConfig clusterConfig = null;
        try {
            clusterConfig = ClusterConfig.ParseConfig("./config.json");
        } catch (MissingArgumentException | IOException | ParseException e) {
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
        String userDirectory = System.getProperty("user.dir");
        log.info("Current dir: " + userDirectory);

        Instant start = Instant.now();
        //JavaRDD<String> rawSummary = sc.textFile(clusterConfig.getHdfsURL()
        //        + "/input/somministrazioni-vaccini-summary-latest.csv");
        Dataset<Row> datasetSummary = spark.read().parquet("hdfs://hdfs-master:54310"
                + "/sabd/input/somministrazioni-vaccini-summary-latest.parquet");

        JavaRDD<Row> rawSummary = datasetSummary.toJavaRDD();

        JavaPairRDD<Date, Tuple2<String, Long>> parsedSummary = rawSummary.mapToPair((row -> {
            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(row.getString(0));
            return new Tuple2<>(date, new Tuple2<>(row.getString(1), row.getLong(2)));
        })).sortByKey(true).cache();

        // Filter the elements before the 1st of January
        parsedSummary = parsedSummary.filter(row -> !row._1.before(firstJanuary));

        JavaPairRDD<Tuple2<Date, String>, Long> vaccinePerMonthArea = parsedSummary.mapToPair((line) -> {
            Calendar cal = Calendar.getInstance();
            cal.setTime(line._1);
            cal.set(Calendar.DAY_OF_MONTH, 1);
            return new Tuple2<>(new Tuple2<>(cal.getTime(), line._2._1), line._2._2);
        }).reduceByKey(Long::sum);

        JavaPairRDD<String, Tuple2<Date, Long>> vaccinePerArea = vaccinePerMonthArea.mapToPair((line) ->
                new Tuple2<>(line._1._2, new Tuple2<>(line._1._1, line._2)));

        Dataset<Row> datasetCenters = spark.read().parquet("hdfs://hdfs-master:54310"
                + "/sabd/input/punti-somministrazione-tipologia.parquet");

        JavaPairRDD<String, Integer> vaccineCenters = datasetCenters.toJavaRDD().mapToPair((row) ->
                new Tuple2<> (row.getString(0), 1)).reduceByKey(Integer::sum);

        JavaPairRDD<String, Tuple2<Tuple2<Date, Long>, Integer>> vaccinePerAreaMonthCenters =
                vaccinePerArea.join(vaccineCenters);

        JavaRDD<Row> result = vaccinePerAreaMonthCenters.mapToPair((line) ->
                new Tuple2<>(new Tuple2<>(line._2._1._1, line._1), line._2._1._2 / line._2._2))
                .sortByKey(valuesComparator)
                .map( line -> RowFactory.create(outputFormat.format(line._1._1), line._1._2, line._2));


        Dataset<Row> clusterDF = spark.createDataFrame(result, resultStruct);
        clusterDF.write()
                .format("csv")
                .option("header", true)
                .mode(SaveMode.Overwrite)
                .save("hdfs://hdfs-master:54310"
                        + "/sabd/output/query1Result");
        Instant end = Instant.now();

        log.info("Query completed in " + Duration.between(start, end).toMillis() + "ms");

        try {
            TimeUnit.MINUTES.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        spark.close();
    }
}
