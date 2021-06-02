package it.uniroma2.ing.dicii.sabd.queries;

import it.uniroma2.ing.dicii.sabd.kmeans.KMeansAlgorithm;
import it.uniroma2.ing.dicii.sabd.kmeans.KMeansBenchmark;
import it.uniroma2.ing.dicii.sabd.kmeans.KMeansType;
import it.uniroma2.ing.dicii.sabd.utils.io.HdfsIO;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;


public class Query3 implements Query {

    private static final Date dateFirstJune2021 = new GregorianCalendar(2021, Calendar.JUNE, 1).getTime();
    private static final long timestampFirstJune2021 = dateFirstJune2021.getTime() / 1000;
    private static final SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final String vaccineAdministrationSummaryFile = "somministrazioni-vaccini-summary-latest.parquet";
    private static final String populationPerRegion = "totale-popolazione.parquet";
    private static final String resultDir = "query3Result";
    private static final String benchmarkFile = "query3Benchmark.csv";

    private static final StructType resultStruct = DataTypes.createStructType(Arrays.asList(
                    DataTypes.createStructField("algoritmo", DataTypes.StringType, false),
                    DataTypes.createStructField("k", DataTypes.IntegerType, false),
                    DataTypes.createStructField("regione", DataTypes.StringType, false),
                    DataTypes.createStructField("stima percentuale popolazione vaccinata", DataTypes.DoubleType, false),
                    DataTypes.createStructField("stima numero vaccinazioni", DataTypes.LongType, false),
                    DataTypes.createStructField("cluster", DataTypes.IntegerType, false)));


    private final Logger log;

    private QueryContext queryContext;
    private HdfsIO hdfsIO;

    public Query3() {
        this.log = LogManager.getLogger(getClass().getSimpleName());
    }

    @Override
    public void configure(QueryContext queryContext, HdfsIO hdfsIO) {
        this.queryContext = queryContext;
        this.hdfsIO = hdfsIO;
    }

    @Override
    public Long execute() {
        log.info("Starting processing query");
        Instant start = Instant.now();

        JavaRDD<Row> regionPopulationRaw = this.hdfsIO.readParquetAsRDD(populationPerRegion);

        /*  Ottengo [(Regione, Popolazione)] dal file totale-popolazione.parquet */
        JavaPairRDD<String,Long> regionPopulation = regionPopulationRaw.mapToPair(line ->
                new Tuple2<>(line.getString(0).split(" /")[0], Long.parseLong(line.getString(1))));

        JavaPairRDD<Date, Tuple2<String, Long>> parsedSummary = this.queryContext.getVaccineAdministrationSummary();

        if (parsedSummary == null) {
            log.info("Not cached");
            JavaRDD<Row> rawSummary = this.hdfsIO.readParquetAsRDD(vaccineAdministrationSummaryFile);

            parsedSummary = rawSummary
                    .mapToPair((row ->
                            new Tuple2<>(inputFormat.parse(row.getString(0)),
                                    new Tuple2<>(row.getString(2).split(" /")[0], Long.parseLong(row.getString(1)) ))))
                    .sortByKey(true);

            parsedSummary = this.queryContext.cacheVaccineAdministration(parsedSummary);
        }

        /*
         * Ottengo [(Regione, (Data, Vaccinazioni))]
         * Per ogni Regione esiste una coppia (Data, Vaccinazioni) per ogni Data
         * Ho eliminato le righe con data successiva al Primo Maggio (da cambiare con il 1 Giugno)
         * Eseguo il caching
         * */
        JavaPairRDD<String, Tuple2<Date, Long>> regionDateVaccinations = parsedSummary.mapToPair(line ->
                new Tuple2<>(line._2._1, new Tuple2<>(line._1, line._2._2)))
                .filter(line -> line._2._1.before(dateFirstJune2021))
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
                line -> new Tuple2<>(line._1, (long) line._2.predict(timestampFirstJune2021)));

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
         * Ottengo [(Regione, Numero Vaccinati, PercentualeVaccinati)] per ogni Regione si tiene la stima per eccesso della percentuale
         * della popolazione di quella regione vaccinata.
         * La lista è ordinata ordinando le regioni per ordine alfabetico.
         * La stima è per eccesso perché si calcola considerando il  rapporto tra le vaccinazioni e la popolazione, ma
         * così facendo si suppone che (1) tutte le vaccinazioni siano state somministrate a persone diverse e che
         * (2) questo abbia reso tali persone vaccinate. */
        JavaPairRDD<String, Tuple2<Long, Double>> regionVaccinationsPercentage = regionVaccinationsTotal
                .join(regionPopulation)
                .mapToPair(line -> new Tuple2<>(line._1, new Tuple2<>(line._2._1, (double) line._2._1 / line._2._2)));

        /*
         * Ottengo [(Regione, Numero vaccinati, PercentualeVaccinati, Vettore)]. Il numero di vaccinati è messo all'interno di un oggetto Vector,
         * utilizzato per eseguire l'algoritmo di KMeans
         */
        JavaPairRDD<String, Tuple3<Long, Double, Vector>> regionVaccinationsTotalVector = regionVaccinationsPercentage
                .mapToPair(line -> new Tuple2<>(line._1, new Tuple3<>(line._2._1, line._2._2, Vectors.dense(line._2._2))))
                .sortByKey()
                .cache();

        JavaRDD<Vector> dataset = regionVaccinationsTotalVector.map(line -> line._2._3()).cache();

        //List<Tuple3<KMeansType, Integer, JavaRDD<Row>>> results = new ArrayList<>();
        List<JavaRDD<Row>> results = new ArrayList<>();
        List<KMeansBenchmark> benchmarkResults = new ArrayList<>();

        for (KMeansType algorithm: KMeansType.values()) {
            log.info("Algorithm: " + algorithm.toString());
            try {
                Class<?> cls = Class.forName(algorithm.getAlgorithmClass());
                Constructor<?> constructor = cls.getConstructor();
                for (int k = 2; k <= 5; k++) {
                    Instant startTraining = Instant.now();
                    KMeansAlgorithm kMeansAlgorithm = (KMeansAlgorithm) constructor.newInstance();
                    kMeansAlgorithm.train(dataset, k, 10);
                    Instant endTraining = Instant.now();
                    JavaPairRDD<String, Tuple3<Long,Double,Integer>> regionCluster = regionVaccinationsTotalVector.mapToPair(line ->
                            new Tuple2<>(line._1, new Tuple3<>(line._2._1(), line._2._2(), kMeansAlgorithm.predict(line._2._3()))));
                    int finalK = k;
                    JavaRDD<Row> regionClusterResult = regionCluster.map(line -> RowFactory.create(algorithm.toString(),
                            finalK, line._1, line._2._2(), line._2._1(), line._2._3()));
                    results.add(regionClusterResult);
                    KMeansBenchmark benchmarkResult = new KMeansBenchmark(algorithm, k,
                            Duration.between(startTraining,endTraining).toMillis(),
                            kMeansAlgorithm.trainingCost());
                    benchmarkResults.add(benchmarkResult);

                }
            } catch (ClassNotFoundException e) {
                log.error("Class not found: " + e.getMessage());
            } catch ( NoSuchMethodException | InvocationTargetException
                    | InstantiationException | IllegalAccessException e) {
                log.error(e.getMessage());
            }
        }

        JavaRDD<Row> queryResult = results.remove(0);

        // Saving query results
        for (JavaRDD<Row> result: results) {
            queryResult = queryResult.union(result);
        }
        this.hdfsIO.saveRDDasCSV(queryResult, resultStruct, resultDir);

        // Saving Benchmark results
        try {
            this.hdfsIO.saveStructAsCSV(benchmarkResults, benchmarkFile);
        } catch (IOException e) {
           log.error("Error during benchmark saving: " + e.getMessage());
        }

        Instant end = Instant.now();
        Long duration = Duration.between(start, end).toMillis();
        log.info("Query completed in " + duration + "ms");

        return duration;

    }
}
