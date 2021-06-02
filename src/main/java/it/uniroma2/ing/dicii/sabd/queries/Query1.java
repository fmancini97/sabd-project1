package it.uniroma2.ing.dicii.sabd.queries;

import it.uniroma2.ing.dicii.sabd.utils.comparators.Tuple2Comparator;
import it.uniroma2.ing.dicii.sabd.utils.io.HdfsIO;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;


public class Query1 {

    private static final Tuple2Comparator<Date, String> valuesComparator = new Tuple2Comparator<>(Comparator.<Date>naturalOrder(),
            Comparator.<String>naturalOrder());
    private static final Date firstJanuary = new GregorianCalendar(2021, Calendar.JANUARY, 1).getTime();
    private static final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final String vaccineAdministrationSummaryFile = "somministrazioni-vaccini-summary-latest.parquet";
    private static final String vaccineCentersFile = "punti-somministrazione-tipologia.parquet";
    private static final String resultFile = "query1Result";


    public static Long execute(QueryContext queryContext, HdfsIO hdfsIO) {
        Logger log = LogManager.getLogger(Query1.class.getSimpleName());
        log.info("Starting processing query");

        Instant start = Instant.now();

        JavaPairRDD<Date, Tuple2<String, Long>> parsedSummary = queryContext.getVaccineAdministrationSummary();

        if (parsedSummary == null) {
            JavaRDD<Row> rawSummary = hdfsIO.readParquetAsRDD(vaccineAdministrationSummaryFile);

            parsedSummary = rawSummary
                    .mapToPair((row ->
                            new Tuple2<>(inputFormat.parse(row.getString(0)),
                                    new Tuple2<>(row.getString(2).split(" /")[0], Long.parseLong(row.getString(1))))))
                    .sortByKey(true);

            parsedSummary = queryContext.cacheVaccineAdministration(parsedSummary);
        }

        parsedSummary = parsedSummary.filter(row -> !row._1.before(firstJanuary));


        JavaPairRDD<Tuple2<Date, String>, Tuple2<Long, Integer>> vaccinePerMonthArea = parsedSummary.mapToPair((line) -> {
            Calendar cal = Calendar.getInstance();
            cal.setTime(line._1);
            cal.set(Calendar.DAY_OF_MONTH, 1);
            return new Tuple2<>(new Tuple2<>(cal.getTime(), line._2._1), new Tuple2<>(line._2._2, 1));
        }).reduceByKey((a,b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        List<Tuple2<Tuple2<Date, String>, Tuple2<Long, Integer>>> results = vaccinePerMonthArea.collect();
        for (Tuple2<Tuple2<Date, String>, Tuple2<Long, Integer>> result: results) {
            log.info(result);
        }

        JavaPairRDD<Tuple2<Date, String>, Long> vaccinePerDayArea = vaccinePerMonthArea.mapToPair((line) ->
                new Tuple2<>(line._1, line._2._1 / line._2._2));

        JavaPairRDD<String, Tuple2<Date, Long>> vaccinePerArea = vaccinePerDayArea.mapToPair((line) ->
                new Tuple2<>(line._1._2, new Tuple2<>(line._1._1, line._2)));


        JavaRDD<Row> datasetCenters = hdfsIO.readParquetAsRDD(vaccineCentersFile);

        JavaPairRDD<String, Integer> vaccineCenters = datasetCenters.mapToPair((row) ->
                new Tuple2<> (row.getString(0).split(" /")[0], 1)).reduceByKey(Integer::sum);

        JavaPairRDD<String, Tuple2<Tuple2<Date, Long>, Integer>> vaccinePerAreaMonthCenters =
                vaccinePerArea.join(vaccineCenters);

        JavaRDD<Row> result = vaccinePerAreaMonthCenters.mapToPair((line) ->
                new Tuple2<>(new Tuple2<>(line._2._1._1, line._1), line._2._1._2 / line._2._2))
                .sortByKey(valuesComparator)
                .map( line -> RowFactory.create(outputFormat.format(line._1._1), line._1._2, line._2));

        List<Tuple3<String, DataType, Boolean>> header = Arrays.asList(
                new Tuple3<>("data", DataTypes.StringType, false),
                new Tuple3<>("regione", DataTypes.StringType, false),
                new Tuple3<>("vaccinazioni per centro", DataTypes.LongType, false));

        hdfsIO.saveRDDAsCSV(result, header, resultFile);

        Instant end = Instant.now();

        log.info("Query completed in " + Duration.between(start, end).toMillis() + "ms");

        return Duration.between(start, end).toMillis();

    }
}
