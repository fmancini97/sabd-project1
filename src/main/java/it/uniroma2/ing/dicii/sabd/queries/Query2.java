package it.uniroma2.ing.dicii.sabd.queries;

import it.uniroma2.ing.dicii.sabd.utils.comparators.Tuple3Comparator;
import it.uniroma2.ing.dicii.sabd.utils.io.HdfsIO;
import it.uniroma2.ing.dicii.sabd.utils.regression.SimpleRegressionWrapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Runs Query2 using Spark Core
 *
 * */
public class Query2 {

    private static final Tuple3Comparator<Date,String,Double> tuple3Comparator =
            new Tuple3Comparator<>(Comparator.<Date>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<Double>naturalOrder());
    private static final Date dateFirstFeb2021 = new GregorianCalendar(2021, Calendar.FEBRUARY, 1).getTime();
    private static final Date dateFirstJun2021 = new GregorianCalendar(2021, Calendar.JUNE, 1).getTime();
    private static final SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat simpleMonthFormat = new SimpleDateFormat("MM");
    private static final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final String vaccineAdministrationFile = "somministrazioni-vaccini-latest.parquet";
    private static final String resultFile = "query2Result";

    private static final StructType resultStruct = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("data", DataTypes.StringType, false),
            DataTypes.createStructField("fascia anagrafica", DataTypes.StringType, false),
            DataTypes.createStructField("regione", DataTypes.StringType, false),
            DataTypes.createStructField("vaccinazioni previste", DataTypes.IntegerType, false)));

    /**
     * @param queryContext: object that holds information about sparkSession
     * @param hdfsIO: object that handles IO with HDFS
     * */
    public static Long execute(QueryContext queryContext, HdfsIO hdfsIO) {
        Logger log = LogManager.getLogger(Query2.class.getSimpleName());
        log.info("Starting processing query");
        Instant start = Instant.now();

        JavaRDD<Row> rawSummary = hdfsIO.readParquetAsRDD(vaccineAdministrationFile);

        // [k:(data, regione, età), v: vaccini]
        JavaPairRDD<Tuple3<Date, String, String>, Double> dateRegionAgeVaccinations = rawSummary.mapToPair(line ->{
            Date date = inputFormat.parse(line.getString(0));
            String region = line.getString(3);
            String age = line.getString(1);
            Double vaccinations = Double.parseDouble(line.getString(2));
            return new Tuple2<>(new Tuple3<>(date,age,region), vaccinations);
        });

        dateRegionAgeVaccinations = dateRegionAgeVaccinations.filter(record -> {
            Date date = record._1._1();
            return !(date.before(dateFirstFeb2021)) && date.before(dateFirstJun2021);
        });


        /* To obtain the uniqueness of the key,
         * it is necessary to manage the existence of vaccines of different brands. */
        dateRegionAgeVaccinations = dateRegionAgeVaccinations.reduceByKey(Double::sum);


        /* [k :(mese, regione, età),v: (data, vaccinazioni)]
         * Keys are duplicated - records with the same key differ on the value "data". */
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

        // [k: (anno_mese, regione, fascia_anagrafica), v: regressore]
        JavaPairRDD<Tuple3<String, String, String>, SimpleRegressionWrapper> monthRegionAgeRegressor =
                monthRegionAgeDateVaccinations.mapToPair(
                        line -> {
                            SimpleRegressionWrapper simpleRegression = new SimpleRegressionWrapper();
                            simpleRegression.addData((double) (line._2._1.getTime() / 1000), line._2._2);
                            return new Tuple2<>(line._1, simpleRegression);
                        });

        monthRegionAgeRegressor = monthRegionAgeRegressor.reduceByKey((a,b) -> {
            a.append(b);
            return a;
        }).filter(line -> line._2.getCounter()>=2); // Filtering regressions which have less than 2 observations

        // [k: (anno_mese, fascia_anagrafica, vaccinazioni), v: regione]
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
                    Date dateFirstDayNextMonth = inputFormat.parse(stringFirstDayNextMonth);
                    double predictedVaccinations = record._2.predict((double)(dateFirstDayNextMonth).getTime() / 1000);
                    Tuple3<Date,String,Double> newKey = new Tuple3<>(dateFirstDayNextMonth, record._1._2(),predictedVaccinations);
                    String region = record._1._3();
                    return new Tuple2<>(newKey, region);
                });

        dateAgePredictedVaccinationsRegion = dateAgePredictedVaccinationsRegion
                .sortByKey(tuple3Comparator,false,1)
                .cache();

        //[(data, fascia_anagrafica)]
        List<Tuple2<Date,String>> dateAgeKeys = dateAgePredictedVaccinationsRegion.map(
                record -> {
                    Date date = record._1._1();
                    String age = record._1._2();
                    return new Tuple2<>(date, age);
                })
                .distinct()
                .sortBy((Function<Tuple2<Date, String>, String>) value -> inputFormat.format(value._1())+value._2(),
                        true, 1)
                .collect();
        // [k: ((anno_mese, fascia_anagrafica, vaccinazioni_previste), regione), v: rank]
        JavaPairRDD<Tuple2<Tuple3<Date, String, Double>, String>, Long> resultsRDD = null;
        for(Tuple2<Date,String> key: dateAgeKeys) {
            JavaPairRDD<Tuple3<Date, String, Double>, String> filteredData = dateAgePredictedVaccinationsRegion
                    .filter(record -> record._1._1().equals(key._1)&&record._1._2().equals(key._2));

            JavaPairRDD<Tuple2<Tuple3<Date, String, Double>, String>, Long> zippedData = filteredData.zipWithIndex();
            /*log.info("Valori chiave: " + key.toString());
            for(Tuple2<Tuple2<Tuple3<Date, String, Double>, String>, Long> elem: zippedData.collect()){
                log.info(elem);
            }*/
            JavaPairRDD<Tuple2<Tuple3<Date, String, Double>, String>, Long> top5 = zippedData.filter(record -> record._2<5);
            if(resultsRDD == null)
                resultsRDD = top5;
            else
                resultsRDD = resultsRDD.union(top5);
        }

        assert resultsRDD != null;
        JavaRDD<Row> resultRow = resultsRDD.map(record ->
                RowFactory.create(outputFormat.format(record._1._1._1()),
                        record._1._1._2(), record._1._2, record._1._1._3().intValue()));

        hdfsIO.saveRDDasCSV(resultRow, resultStruct, resultFile);

        Instant end = Instant.now();
        Long duration = Duration.between(start, end).toMillis();
        log.info("Query completed in " + duration + "ms");
        return duration;
    }

}
