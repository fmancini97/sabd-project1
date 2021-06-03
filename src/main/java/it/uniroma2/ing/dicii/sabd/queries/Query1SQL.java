package it.uniroma2.ing.dicii.sabd.queries;

import it.uniroma2.ing.dicii.sabd.utils.io.HdfsIO;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import static org.apache.spark.sql.functions.*;
import static scala.collection.JavaConverters.asScalaBuffer;


/**
 * Runs Query1 using Spark SQL
 *
 * */
public class Query1SQL {

    private static final String vaccineAdministrationSummaryFile = "somministrazioni-vaccini-summary-latest.parquet";
    private static final String vaccineCentersFile = "punti-somministrazione-tipologia.parquet";
    private static final String resultFile = "query1SQLResult";
    private static final String firstJanuaryString = "2021-01-01";
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM");

    /**
     * @param queryContext: object that holds information about sparkSession
     * @param hdfsIO: object that handles IO with HDFS
     * */
    public static Long execute(QueryContext queryContext, HdfsIO hdfsIO) {

        Logger log = LogManager.getLogger(Query1SQL.class.getSimpleName());
        SparkSession sparkSession = queryContext.getSparkSession();
        log.info("Starting processing query");
        Instant start = Instant.now();

        // [(data, vaccinazioni, regione)]
        Dataset<Row> dataframe = hdfsIO.readParquetAsDataframe(vaccineAdministrationSummaryFile);


        dataframe = dataframe.withColumn("data_somministrazione", to_date(dataframe.col("data_somministrazione")));
        dataframe = dataframe.withColumn("totale", dataframe.col("totale").cast("long"));


        dataframe = dataframe.filter(dataframe.col("data_somministrazione").geq(lit(firstJanuaryString)));

        UserDefinedFunction nameParser = udf((String nome_area) -> nome_area.split(" /")[0], DataTypes.StringType);
        dataframe = dataframe.withColumn("nome_area", nameParser.apply(dataframe.col("nome_area")));




        UserDefinedFunction dateConverter = udf((Date data_somministrazione) -> {
            Calendar cal = Calendar.getInstance();
            cal.setTime(data_somministrazione);
            cal.set(Calendar.DAY_OF_MONTH,1);
            return new java.sql.Date(cal.getTimeInMillis());
        }, DataTypes.DateType);
        dataframe = dataframe.withColumn("data", dateConverter.apply(dataframe.col("data_somministrazione")));

        dataframe.createOrReplaceTempView("vaccineAdministrationSummaryTable");

        // [(anno_mese, regione, vaccinazioni, numero_osservazioni)]
        dataframe = sparkSession.sql("SELECT data, nome_area, sum(totale) as totale_vaccinazioni, " +
                "count(totale) as numero_osservazioni FROM vaccineAdministrationSummaryTable GROUP BY data, nome_area");

        // [(regione, numero_centri)]
        Dataset<Row> dataframeCenters = hdfsIO.readParquetAsDataframe(vaccineCentersFile);
        dataframeCenters = dataframeCenters.withColumn("nome_area", nameParser.apply(dataframeCenters.col("nome_area")));
        dataframeCenters.createOrReplaceTempView("vaccineCenters");
        dataframeCenters = sparkSession.sql("SELECT nome_area, count(*) as numero_centri FROM vaccineCenters" +
                " GROUP BY nome_area");

        dataframe = dataframe.join(dataframeCenters, asScalaBuffer(Collections.singletonList("nome_area")), "inner");

        // [(anno_mese, regione, vaccinazioni, numero_osservazioni, numero_centri, vaccinazioni_per_centro)]
        dataframe = dataframe.withColumn("vaccinazioni_per_centro", dataframe.col("totale_vaccinazioni")
                .cast("double").divide(dataframe.col("numero_centri").cast("double"))
                .divide(dataframe.col("numero_osservazioni").cast("double")));


        dataframe.createOrReplaceTempView("vaccineAdministrationSummaryTable");
        // [(anno_mese, regione, vaccinazioni_per_centro)]
        dataframe = sparkSession.sql("SELECT data, nome_area as regione, vaccinazioni_per_centro " +
                "FROM vaccineAdministrationSummaryTable ORDER BY data,regione");

        dataframe = dataframe.withColumn("vaccinazioni_per_centro", dataframe.col("vaccinazioni_per_centro").cast("long"));


        hdfsIO.saveDataframeAsCSV(dataframe, resultFile);

        Instant end = Instant.now();

        log.info("Query completed in " + Duration.between(start, end).toMillis() + "ms");

        return Duration.between(start, end).toMillis();
    }

}
