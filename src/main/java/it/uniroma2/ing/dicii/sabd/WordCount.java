package it.uniroma2.ing.dicii.sabd;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.regex.Pattern;


public class WordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args){
        Logger log = LogManager.getLogger("SABD-PROJECT");
        String outputPath = "output";
        if (args.length > 0)
            outputPath = args[0];

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Instant start = Instant.now();
        /*
         * As data source, we can use a file stored on the local file system
          * or on the HDFS, or we can parallelize
         */
        JavaRDD<String> input = sc.textFile("hdfs://hdfs-master:54310/input/mytext");
//        JavaRDD<String> input = sc.textFile("input");
/*        JavaRDD<String> input = sc.parallelize(Arrays.asList(
                                        "if you prick us do we not bleed",
                                        "if you tickle us do we not laugh",
                                        "if you poison us do we not die and",
                                        "if you wrong us shall we not revenge"
                                    ));
*/
        // Transformations
        JavaRDD<String> words = input.flatMap(line -> Arrays.asList(SPACE.split(line)).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((x, y) -> x+y);
        // Action
        /* Trasformations are lazy, and they are applied only when a action
         * should be performed on a RDD.                                            */

        counts.saveAsTextFile("hdfs://hdfs-master:54310/" + outputPath);
        Instant end = Instant.now();
        long timeElapsed = Duration.between(start, end).toMillis();
        log.info("Query completed in " + timeElapsed + "ms");

        sc.stop();
    }

}
