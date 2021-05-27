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


public class Query1 implements Query {

    private final Logger log;
    private static final Tuple2Comparator<Date, String> valuesComparator = new Tuple2Comparator<>(Comparator.<Date>naturalOrder(),
            Comparator.<String>naturalOrder());
    private static final Date firstJanuary = new GregorianCalendar(2021, Calendar.JANUARY, 1).getTime();
    private static final SimpleDateFormat outputFormat = new SimpleDateFormat("MMMM yyyy", Locale.ITALIAN);
    private static final SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final String vaccineAdministrationSummaryFile = "somministrazioni-vaccini-summary-latest.parquet";
    private static final String vaccineCentersFile = "punti-somministrazione-tipologia.parquet";
    private static final String resultFile = "query1Result";
    private QueryContext queryContext;
    private HdfsIO hdfsIO;

    public Query1() {
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

        JavaPairRDD<Date, Tuple2<String, Long>> parsedSummary = this.queryContext.getVaccineAdministrationSummary();

        if (parsedSummary == null) {
            JavaRDD<Row> rawSummary = this.hdfsIO.readParquet(vaccineAdministrationSummaryFile);

            parsedSummary = rawSummary
                    .mapToPair((row ->
                            new Tuple2<>(inputFormat.parse(row.getString(0)),
                                    new Tuple2<>(row.getString(2).split(" /")[0], Long.parseLong(row.getString(1))))))
                    .sortByKey(true);

            parsedSummary = this.queryContext.cacheVaccineAdministration(parsedSummary);
        }

        parsedSummary = parsedSummary.filter(row -> !row._1.before(firstJanuary));

        JavaPairRDD<Tuple2<Date, String>, Long> vaccinePerMonthArea = parsedSummary.mapToPair((line) -> {
            Calendar cal = Calendar.getInstance();
            cal.setTime(line._1);
            cal.set(Calendar.DAY_OF_MONTH, 1);
            return new Tuple2<>(new Tuple2<>(cal.getTime(), line._2._1), line._2._2);
        }).reduceByKey(Long::sum);

        JavaPairRDD<String, Tuple2<Date, Long>> vaccinePerArea = vaccinePerMonthArea.mapToPair((line) ->
                new Tuple2<>(line._1._2, new Tuple2<>(line._1._1, line._2)));


        JavaRDD<Row> datasetCenters = this.hdfsIO.readParquet(vaccineCentersFile);

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

        this.hdfsIO.saveRDDAsCSV(result, header, resultFile);

        Instant end = Instant.now();

        log.info("Query completed in " + Duration.between(start, end).toMillis() + "ms");

        return Duration.between(start, end).toMillis();

    }
}
