package it.uniroma2.ing.dicii.sabd.queries;

import it.uniroma2.ing.dicii.sabd.utils.comparators.Tuple3Comparator;
import it.uniroma2.ing.dicii.sabd.utils.io.HdfsIO;
import it.uniroma2.ing.dicii.sabd.utils.wrappers.SimpleRegressionWrapper;
import org.apache.commons.math3.stat.regression.SimpleRegression;
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


public class Query2 implements Query {

    private static final Tuple3Comparator<Date,String,Double> tuple3Comparator =
            new Tuple3Comparator<>(Comparator.<Date>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<Double>naturalOrder());
    private static final Date dateFirstFeb2021 = new GregorianCalendar(2021, Calendar.FEBRUARY, 1).getTime();
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


    private final Logger log;

    private QueryContext queryContext;
    private HdfsIO hdfsIO;

    public Query2() {
        this.log = LogManager.getLogger(getClass().getSimpleName());
    }

    @Override
    public void configure(QueryContext queryContext, HdfsIO hdfsIO) {
        this.queryContext = queryContext;
        this.hdfsIO = hdfsIO;
    }

    @Override
    public Long execute() {

        this.log.info("Starting processing query");
        Instant start = Instant.now();

        JavaRDD<Row> rawSummary = this.hdfsIO.readParquet(vaccineAdministrationFile);
        /*  Ottengo [(data, regione, età), vaccini]*/
        JavaPairRDD<Tuple3<Date, String, String>, Double> dateRegionAgeVaccinations = rawSummary.mapToPair(line ->{
            Date date = inputFormat.parse(line.getString(0));
            String region = line.getString(3);
            String age = line.getString(1);
            Double vaccinations = Double.parseDouble(line.getString(2));
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

        JavaPairRDD<Tuple3<String, String, String>, SimpleRegressionWrapper> monthRegionAgeRegressor =
                monthRegionAgeDateVaccinations.mapToPair(
                        line -> {
                            SimpleRegressionWrapper simpleRegression = new SimpleRegressionWrapper();
                            simpleRegression.addData((double) (line._2._1.getTime()), line._2._2);
                            return new Tuple2<>(line._1, simpleRegression);
                        });

        monthRegionAgeRegressor = monthRegionAgeRegressor.reduceByKey((a,b) -> {
            a.append(b);
            return a;
        }).filter(line -> line._2.getCounter()>=2); // Filtering regressions which have less than 2 observations


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
                    double predictedVaccinations = record._2.predict((double)(dateFirstDayNextMonth).getTime());
                    Tuple3<Date,String,Double> newKey = new Tuple3<>(dateFirstDayNextMonth, record._1._2(),predictedVaccinations);
                    String region = record._1._3();
                    return new Tuple2<>(newKey, region);
                });

        dateAgePredictedVaccinationsRegion = dateAgePredictedVaccinationsRegion
                .sortByKey(tuple3Comparator,false,1)
                .cache();

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

        this.hdfsIO.saveRDDasCSV(resultRow, resultStruct, resultFile);

        Instant end = Instant.now();
        Long duration = Duration.between(start, end).toMillis();
        log.info("Query completed in " + duration + "ms");
        return duration;
    }

}
