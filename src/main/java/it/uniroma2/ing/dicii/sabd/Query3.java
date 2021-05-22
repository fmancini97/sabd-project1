package it.uniroma2.ing.dicii.sabd;

import it.uniroma2.ing.dicii.sabd.kmeans.KMeansAlgorithm;
import it.uniroma2.ing.dicii.sabd.kmeans.KMeansType;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import scala.Tuple3;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Query3 {

    public static void main(String[] args)  {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date dateFirstMay2021 = new GregorianCalendar(2021, Calendar.MAY, 1).getTime(); // TODO Cambiare con il primo giugno
        long timestampFirstMay2021 = dateFirstMay2021.getTime() / 1000;
        List<StructField> resultfields = new ArrayList<>();
        resultfields.add(DataTypes.createStructField("region", DataTypes.StringType, false));
        resultfields.add(DataTypes.createStructField("cluster", DataTypes.IntegerType, false));
        StructType resultStruct = DataTypes.createStructType(resultfields);

        List<StructField> benchmarkFields = new ArrayList<>();
        benchmarkFields.add(DataTypes.createStructField("algorithm", DataTypes.StringType, false));
        benchmarkFields.add(DataTypes.createStructField("k", DataTypes.IntegerType, false));
        benchmarkFields.add(DataTypes.createStructField("training cost", DataTypes.DoubleType, false));
        benchmarkFields.add(DataTypes.createStructField("WSSSE", DataTypes.DoubleType, false));
        StructType benchmarkStruct = DataTypes.createStructType(benchmarkFields);

        Logger log = LogManager.getLogger("SABD-PROJECT");

        SparkSession spark = SparkSession
                .builder()
                .appName("Query3")
                .getOrCreate();

        log.info("Starting processing query");

        Instant start = Instant.now();

        /*  Ottengo [(Regione, Popolazione)] dal file totale-popolazione.parquet */
        JavaPairRDD<String,Long> regionPopulation = spark.read().parquet("hdfs://hdfs-master:54310"
                + "/sabd/input/totale-popolazione.parquet").toJavaRDD().mapToPair(line ->
                new Tuple2<>(line.getString(0).split(" /")[0],
                        line.getLong(1)));

        /*
        List<Tuple2<String, Long>> regionPopulationLists = regionPopulation.collect();
        for (Tuple2<String, Long> regionPopulationList: regionPopulationLists) {
            log.info(regionPopulationList);
        }*/

        Dataset<Row> datasetSummary = spark.read().parquet("hdfs://hdfs-master:54310"
                + "/sabd/input/somministrazioni-vaccini-summary-latest.parquet");

        JavaRDD<Row> rawSummary = datasetSummary.toJavaRDD();

        /*
         * Ottengo [(Regione, (Data, Vaccinazioni))]
         * Per ogni Regione esiste una coppia (Data, Vaccinazioni) per ogni Data
         * Ho eliminato le righe con data successiva al Primo Maggio (da cambiare con il 1 Giugno)
         * Eseguo il caching // TODO utilizzare il caching precedente
         * */
        JavaPairRDD<String, Tuple2<Date, Long>> regionDateVaccinations = rawSummary.mapToPair(line ->
                                new Tuple2<>(line.getString(20).split(" /")[0],
                                        new Tuple2<>(simpleDateFormat.parse(line.getString(0)), line.getLong(2))))
                .filter(line -> line._2._1.before(dateFirstMay2021))
                .cache();

        /*
         * Ottengo [(Regione, RegressioneLineare)]
         * Per ogni regione costruisco una Regressione Lineare per la stima del numero del numero di
         * vaccinazioni giornaliere. Ogni regione ha associato più di una Regressione Lineare, ciascuna della quale
         * viene stimata attraverso una sola coppia (Data, # vaccinazioni)
         * */
        JavaPairRDD<String, SimpleRegression> regionRegression = regionDateVaccinations.mapToPair(line -> {
            SimpleRegression simpleRegression = new SimpleRegression();
            simpleRegression.addData((double) (line._2._1.getTime() / 1000), line._2._2);
            return new Tuple2<>(line._1, simpleRegression);
        });
        /*
         * Ottengo [(Regione, RegressioneLineare)]
         * Per ciascuna Regione ottengo una sola regressione lineare che stima il numero di vaccinazioni giornaliere.
         * La combine si basa sulla formula di aggiornamento della media, descritto nell'articolo di Philippe Pébay:
         * Formulas for Robust, One-Pass Parallel Computation of Covariances and Arbitrary-Order Statistical Moments,
         * 2008, Technical Report SAND2008-6212, Sandia National Laboratories.
         * */
        regionRegression = regionRegression.reduceByKey((a,b) -> {
            a.append(b);
            return a;
        });

        /*
         * Ottengo coppie [(Regione, VaccinazioniPredette)], una per ogni regione, con vaccinazioni predette
         * il numero di vaccinazioni predette al 1 Giugno 2021
         */
        JavaPairRDD<String, Long> regionVaccinationsPred = regionRegression.mapToPair(
                line -> new Tuple2<>(line._1, (long) line._2.predict(timestampFirstMay2021)));

        /*
         * Ottengo [(Regione, Vaccinazioni)] per ogni Regione ci sono tante coppie quante sono le date in cui
         * sono stati effettuate delle vaccinazioni fino al 01-06-2021.
         * */
        JavaPairRDD<String, Long> regionVaccinations = regionDateVaccinations.mapToPair(line ->
                new Tuple2<>(line._1, line._2._2))
                .union(regionVaccinationsPred);

        /*
         * Ottengo [(Regione, Vaccinazioni)] per ogni Regione si tiene il valore di tutte le vaccinazioni
         * effettuate fino al 01-06-2021 */
        JavaPairRDD<String, Long> regionVaccinationsTotal = regionVaccinations.reduceByKey(Long::sum);

        /*
         * Ottengo [(Regione, PercentualeVaccinati)] per ogni Regione si tiene la stima per eccesso della percentuale
         * della popolazione di quella regione vaccinata.
         * La lista è ordinata ordinando le regioni per ordine alfabetico.
         * La stima è per eccesso perché si calcola considerando il  rapporto tra le vaccinazioni e la popolazione, ma
         * così facendo si suppone che (1) tutte le vaccinazioni siano state somministrate a persone diverse e che
         * (2) questo abbia reso tali persone vaccinate. */
        JavaPairRDD<String, Double> regionVaccinationsPercentage = regionVaccinationsTotal
                .join(regionPopulation)
                .mapToPair(line -> new Tuple2<>(line._1, (double) line._2._1 / line._2._2));

        /*
         * Ottengo [(Regione, PercentualeVaccinati)]. Il numero di vaccinati è messo all'interno di un oggetto Vector,
         * utilizzato per eseguire l'algoritmo di KMeans
         */
        JavaPairRDD<String, Vector> regionVaccinationsTotalVector = regionVaccinationsPercentage
                .mapToPair(line -> new Tuple2<>(line._1, Vectors.dense(line._2)))
                .sortByKey()
                .cache();

        /*
        List<Tuple2<String, Double>> results1 = regionVaccinationsPercentage.collect();

        log.info("Region Percentage:");
        for (Tuple2<String, Double> result1 : results1) {
            log.info(result1);
        }*/

        JavaRDD<Vector> dataset = regionVaccinationsTotalVector.map(line -> line._2).cache();

        List<Tuple3<KMeansType, Integer, JavaRDD<Row>>> results = new ArrayList<>();
        List<Row> benchmarkResults = new ArrayList<>();

        for (KMeansType algorithm: KMeansType.values()) {
            log.info("Algorithm: " + algorithm.toString());
            try {
                Class<?> cls = Class.forName(algorithm.getAlgorithmClass());
                Constructor<?> constructor = cls.getConstructor();
                for (int k = 2; k <= 5; k++) {
                    KMeansAlgorithm kMeansAlgorithm = (KMeansAlgorithm) constructor.newInstance();
                    kMeansAlgorithm.train(dataset, k, 10);
                    JavaPairRDD<String, Integer> regionCluster = regionVaccinationsTotalVector.mapToPair(line ->
                            new Tuple2<>(line._1, kMeansAlgorithm.predict(line._2)));
                    JavaRDD<Row> regionClusterResult = regionCluster.map(line -> RowFactory.create(line._1, line._2));
                    results.add(new Tuple3<>(algorithm, k, regionClusterResult));
                    Row benchmarkResult = RowFactory.create(algorithm.toString(), k,
                            kMeansAlgorithm.trainingCost(), kMeansAlgorithm.computeCost(dataset));
                    benchmarkResults.add(benchmarkResult);

                    /*List<Tuple2<String, Integer>> results = regionCluster.collect();
                    log.info("K = " + k);
                    for (Tuple2<String, Integer> result: results) {
                        log.info(result);
                    }*/
                }
            } catch (ClassNotFoundException e) {
                log.error("Class not found: " + e.getMessage());
            } catch ( NoSuchMethodException | InvocationTargetException
                    | InstantiationException | IllegalAccessException e) {
                log.error(e.getMessage());
            }
        }

        // Saving results
        for (Tuple3<KMeansType, Integer, JavaRDD<Row>> result: results) {
            Dataset<Row> clusterDF = spark.createDataFrame(result._3(), resultStruct);
            clusterDF.write()
                    .format("csv")
                    .option("header", true)
                    .mode(SaveMode.Overwrite)
                    .save("hdfs://hdfs-master:54310"
                            + "/sabd/output/query3Results/" + result._1().toString() + "-" + result._2());
        }

        for (Row result: benchmarkResults) {
            log.info(result);
        }

        // Saving performance results
        Dataset<Row> clusterDF = spark.createDataFrame(benchmarkResults, benchmarkStruct);
        clusterDF.write()
                .format("csv")
                .option("header", true)
                .mode(SaveMode.Overwrite)
                .save("hdfs://hdfs-master:54310"
                        + "/sabd/output/query3Benchmark");


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
