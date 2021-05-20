package it.uniroma2.ing.dicii.sabd;

import it.uniroma2.ing.dicii.sabd.utils.Algorithm;
import it.uniroma2.ing.dicii.sabd.utils.KMeansAlgorithm;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


public class Query3 {

    public static void main(String[] args)  {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar firstMayCal = Calendar.getInstance();
        firstMayCal.set(Calendar.YEAR, 2021);
        firstMayCal.set(Calendar.MONTH, Calendar.MAY);
        firstMayCal.set(Calendar.DAY_OF_MONTH, 1);
        Date dateFirstMay2021 = firstMayCal.getTime();
        long timestampFirstMay2021 = dateFirstMay2021.getTime() / 1000;


        Logger log = LogManager.getLogger("SABD-PROJECT");

        SparkSession spark = SparkSession
                .builder()
                .appName("Query3")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        log.info("Starting processing query");
        Instant start = Instant.now();


        /*  Ottengo [(Regione, Popolazione)] dal file totale-popolazione.parquet */
        JavaPairRDD<String,Double> regionPopulation = spark.read().parquet("hdfs://hdfs-master:54310"
                + "/input/totale-popolazione.parquet").toJavaRDD().mapToPair(line -> new Tuple2<>(line.getString(0), (double) line.getLong(1)));


        Dataset<Row> datasetSummary = spark.read().parquet("hdfs://hdfs-master:54310"
                + "/input/somministrazioni-vaccini-summary-latest.parquet");

        JavaRDD<Row> rawSummary = datasetSummary.toJavaRDD();

        /*
         * Ottengo [(Regione, (Data, Vaccinazioni))]
         * Per ogni Regione esiste una coppia (Data, Vaccinazioni) per ogni Data
         * Ho eliminato le righe con data successiva al Primo Maggio (da cambiare con il 1 Giugno)
         * Eseguo il caching // TODO utilizzare il caching precedente
         * */
        JavaPairRDD<String, Tuple2<Date, Long>> regionDateVaccinations =
                rawSummary
                        .mapToPair(line ->
                                new Tuple2<>(line.getString(20),
                                        new Tuple2<>(simpleDateFormat.parse(line.getString(0)), line.getLong(2))))
                .filter(line -> line._2._1.before(dateFirstMay2021)).cache();

        JavaPairRDD<String, SimpleRegression> regionRegression = regionDateVaccinations.mapToPair(line -> {
            SimpleRegression simpleRegression = new SimpleRegression();
            simpleRegression.addData((double) (line._2._1.getTime() / 1000), line._2._2);
            return new Tuple2<>(line._1, simpleRegression);
        });

        regionRegression = regionRegression.reduceByKey((a,b) -> {
            a.append(b);
            return a;
        });

        JavaPairRDD<String, Long> regionDateVaccinationsPred = regionRegression.mapToPair(
                line -> new Tuple2<>(line._1, (long) line._2.predict(timestampFirstMay2021)));

        JavaPairRDD<String, Long> regionVaccinations = regionDateVaccinations.mapToPair(line ->
                new Tuple2<>(line._1, line._2._2)).union(regionDateVaccinationsPred);

        JavaPairRDD<String, Long> regionVaccinationsTotal = regionVaccinations.reduceByKey(Long::sum);
        JavaPairRDD<String, Vector> regionVaccinationsTotalVector = regionVaccinationsTotal.mapToPair(
                line -> new Tuple2<>(line._1, Vectors.dense(line._2))).cache();

        JavaRDD<Vector> dataset = regionVaccinationsTotalVector.map(line -> line._2).cache();


        for (Algorithm algorithm: Algorithm.values()) {
            log.info("Algorithm: " + algorithm.toString());
            try {
                Class<?> cls = Class.forName(algorithm.getAlgorithmClass());
                Constructor<?> constructor = cls.getConstructor();

                for (int k = 1; k <= 5; k++) {
                    KMeansAlgorithm kMeansAlgorithm = (KMeansAlgorithm) constructor.newInstance();
                    kMeansAlgorithm.train(dataset, k, 10);
                    JavaPairRDD<String, Integer> regionCluster = regionVaccinationsTotalVector.mapToPair(line -> new Tuple2<>(line._1, kMeansAlgorithm.predict(line._2)));
                    List<Tuple2<String, Integer>> results = regionCluster.collect();
                    log.info("K = " + k);
                    for (Tuple2<String, Integer> result: results) {
                        log.info(result);
                    }
                }

            } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException
                    | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }


        }







        /*
         * Ottengo [(Regione, [(Data, Vaccinazioni)])
         * Per ogni Regione esiste un'unica lista di coppie (Data, Vaccinazioni)
         * NOTE PER LA RELAZIONE:
         * 1) La groupByKey ci garantisce che tutti i dati di una stessa regione sono contenuti in un'unica partizione.
         * 2) La parellelizzazione si ottiene dall'andare ad effettuare regressione sui dati delle diverse regioni
         * in parallelo.
         * Data la dimensione del dataset parallelizzare in questo modo potrebbe avere più senso rispetto ad effettuare
         * un ciclo for sulle regioni e lavorare sui dati di una regione alla volta.
         * Se la quantità di dati di ciascuna regione fosse stata maggiore, probabilmente sarebbe stato meglio
         * effettuare un ciclo for ed andare a parallelizzare la computazione su ciascuna regione perché in questo
         * caso sarebbe troppo oneroso per un singolo worker node andare a processare tutti i dati di una sola regione.
         * */
        //JavaPairRDD<String, Iterable<Tuple2<Date,Double>>> regionDateVaccinationsIterable = regionDateVaccinations.groupByKey();

        /*
         * Ottengo coppie [(Regione, VaccinazioniPredette)], una per ogni regione, con vaccinazioni predette
         * il numero di vaccinazioni predette al 1 Giugno 2021 */
        /*JavaPairRDD<String, Double> regionPredictedVaccinations = regionDateVaccinationsIterable.mapToPair(record -> {
            String region = record._1();
            Iterable<Tuple2<Date,Double>> dateVaccinations = record._2();

            SimpleRegression simpleRegression = new SimpleRegression();
            for(Tuple2<Date, Double> elem: dateVaccinations){
                Date date = elem._1();
                double dateFromEpoch = (double)date.getTime();
                double vaccinations = elem._2();

                simpleRegression.addData(dateFromEpoch, vaccinations);
            }
            double xToBePredicted = (double)simpleDateFormat.parse(FIRSTMAY2021).getTime();
            double yPredicted = simpleRegression.predict(xToBePredicted);

            return new Tuple2<String,Double>(region, yPredicted);
        });*/

        /*
         * Ottengo [(Regione, Vaccinazioni)] per ogni Regione ci sono tante coppie quante sono le date in cui
         * sono stati effettuate delle vaccinazioni fino al 31-05-2021.
         * */
        /*JavaPairRDD<String, Double> regionVaccinations = regionDateVaccinations.mapToPair(record -> {
            String region = record._1();
            Double vaccinations = record._2()._2();
            return new Tuple2<>(region, vaccinations);
        });*/

        /*
         * Ottengo [(Regione, Vaccinazioni)] per ogni Regione ci sono tante coppie quante sono le date in cui
         * sono stati effettuate delle vaccinazioni fino al 01-06-2021.
         * */
        //regionVaccinations = regionVaccinations.union(regionPredictedVaccinations);

        /*
         * Ottengo [(Regione, Vaccinazioni)] per ogni Regione si tiene il valore di tutte le vaccinazioni
         * effettuate fino al 01-06-2021 */
        //regionVaccinations = regionVaccinations.reduceByKey((value1, value2) -> value1+value2);

        /*
         * Ottengo [(Regione, PercentualeVaccinati)] per ogni Regione si tiene la stima per eccesso della percentuale
         * della popolazione di quella regione vaccinata.
         * La lista è ordinata ordinando le regioni per ordine alfabetico.
         * La stima è per eccesso perché si calcola considerando il  rapporto tra le vaccinazioni e la popolazione, ma
         * così facendo si suppone che (1) tutte le vaccinazioni siano state somministrate a persone diverse e che
         * (2) questo abbia reso tali persone vaccinate. */
        /*log.info("149 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        JavaPairRDD<String,Double> regionPercentage = regionVaccinations.join(regionPopulation).mapToPair(line->{
            Double vaccinations = line._2()._1();
            Double population = line._2()._2();
            Double percentage = vaccinations/population;
            return new Tuple2<String, Double>(line._1(),percentage);
        }).sortByKey();
        log.info("156 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        /*  Si effettua caching del seguente RDD perché è usato in tutte le iterazioni di kmeans */
        /*JavaRDD<Vector> vectors = regionPercentage.map(record ->{
            double[] values = new double[1];
            values[0] = record._2();
            return Vectors.dense(values);
        }).cache();

        List<String> clusteringPerformances = new ArrayList<String>();
        String performance = "Algoritmo,Num Clusters,Somma dei quadrati delle distanze,Tempo di processamento(ms)";
        clusteringPerformances.add(performance);
        List<String> clusteringResults = new ArrayList<String>();
        String result = "Algoritmo,Num Clusters,Regione,Stima percentuale vaccinati,Cluster";
        clusteringResults.add(result);
        for(String algorithm: ALGORITHMS){
            for(int k = 2; k<=5; k++){
                int numIterations = 20;
                if(algorithm.equalsIgnoreCase("KMeans")) {

                    final long startTime = System.currentTimeMillis();
                    KMeansModel clusters = KMeans.train(vectors.rdd(), k, numIterations);

                    final long timeElapses = System.currentTimeMillis() - startTime;
                    double sumOfSquaredDistances = clusters.computeCost(vectors.rdd());
                    performance = algorithm+","+k+","+sumOfSquaredDistances+","+timeElapses;
                    clusteringPerformances.add(performance);

                    JavaRDD<Integer> labels = clusters.predict(vectors);
                    List<Integer> labelsList = labels.collect();
                    List<Tuple2<String, Double>> regionPercentageList = regionPercentage.collect();
                    for(int i = 0; i < labelsList.size(); i++){
                        result = algorithm+","+k+","+regionPercentageList.get(i)._1()+","
                                +regionPercentageList.get(i)._2()+","+labelsList.get(i);
                        clusteringResults.add(result);
                    }
                } else if (algorithm.equalsIgnoreCase("BisectingKMeans")){
                    BisectingKMeans bisectingKMeans = new BisectingKMeans();
                    bisectingKMeans.setK(k);
                    final long startTime = System.currentTimeMillis();
                    BisectingKMeansModel clusters = bisectingKMeans.run(vectors.rdd());

                    final long timeElapses = System.currentTimeMillis() - startTime;
                    double sumOfSquaredDistances = clusters.computeCost(vectors.rdd());
                    performance = algorithm+","+k+","+sumOfSquaredDistances+","+timeElapses;
                    clusteringPerformances.add(performance);

                    JavaRDD<Integer> labels = clusters.predict(vectors);
                    List<Integer> labelsList = labels.collect();
                    List<Tuple2<String, Double>> regionPercentageList = regionPercentage.collect();
                    for(int i = 0; i < labelsList.size(); i++){
                        result = algorithm+","+k+","+regionPercentageList.get(i)._1()+","
                                +regionPercentageList.get(i)._2()+","+labelsList.get(i);
                        clusteringResults.add(result);
                    }
                }
            }
        }

        JavaRDD<String> performancesRDD = sparkContext.parallelize(clusteringPerformances);
        JavaRDD<String> resultsRDD = sparkContext.parallelize(clusteringResults);

        performancesRDD.saveAsTextFile("hdfs://hdfs-master:54310" + "/output/query3clusteringPerformances");
        resultsRDD.saveAsTextFile("hdfs://hdfs-master:54310" + "/output/query3clusteringResult");
        Instant end = Instant.now();
        log.info("Query completed in " + Duration.between(start, end).toMillis() + "ms");
        */

        spark.close();

    }
}
