package it.uniroma2.ing.dicii.sabd.queries;

import it.uniroma2.ing.dicii.sabd.utils.io.HdfsIO;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;
import static scala.collection.JavaConverters.asScalaBuffer;

public class Query1SQL {

    private static final String vaccineAdministrationSummaryFile = "somministrazioni-vaccini-summary-latest.parquet";
    private static final String vaccineCentersFile = "punti-somministrazione-tipologia.parquet";
    private static final String resultFile = "query1Result";
    private static final String firstJanuaryString = "2021-01-01";

    public static Long execute(QueryContext queryContext, HdfsIO hdfsIO) {

        Logger log = LogManager.getLogger(Query1SQL.class.getSimpleName());
        SparkSession sparkSession = queryContext.getSparkSession();
        log.info("Starting processing query");
        Instant start = Instant.now();

        Dataset<Row> dataframe = hdfsIO.readParquetAsDataframe(vaccineAdministrationSummaryFile);

     //   dataframe = dataframe.withColumn("nome_area", functions.substring_index(dataframe.col("nome_area"), " /", 1));
        dataframe = dataframe.withColumn("data_somministrazione", to_date(dataframe.col("data_somministrazione")));
        dataframe = dataframe.withColumn("totale", dataframe.col("totale").cast("long"));


        dataframe = dataframe.filter(dataframe.col("data_somministrazione").geq(lit(firstJanuaryString)));

        dataframe = dataframe.withColumn("anno_mese", functions.concat(
                functions.year(dataframe.col("data_somministrazione")), functions.lit("-"), functions.month(dataframe.col("data_somministrazione"))));
        dataframe.createOrReplaceTempView("vaccineAdministrationSummary");

        dataframe = sparkSession.sql("SELECT anno_mese, nome_area, sum(totale) as totale_vaccinazioni" +
                " FROM vaccineAdministrationSummary GROUP BY anno_mese, nome_area");

        Dataset<Row> dataframeCenters = hdfsIO.readParquetAsDataframe(vaccineCentersFile);
      //  dataframeCenters = dataframeCenters.withColumn("nome_area", functions.substring_index(dataframe.col("nome_area"), " /", 1));
        dataframeCenters.createOrReplaceTempView("vaccineCenters");
        dataframeCenters = sparkSession.sql("SELECT nome_area, count(*) as numero_centri FROM vaccineCenters" +
                " GROUP BY nome_area");

        dataframe = dataframe.join(dataframeCenters, asScalaBuffer(Collections.singletonList("nome_area")), "inner");

        dataframe = dataframe.withColumn("vaccinazioni_per_centro", dataframe.col("totale_vaccinazioni")
                .cast("double").divide(dataframe.col("numero_centri").cast("double")));

        dataframe.createOrReplaceTempView("vaccineAdministrationSummary");
        dataframe = sparkSession.sql("SELECT anno_mese as data, nome_area as regione, vaccinazioni_per_centro " +
                "FROM vaccineAdministrationSummary ORDER BY data,regione");

        dataframe = dataframe.withColumn("vaccinazioni_per_centro", dataframe.col("vaccinazioni_per_centro").cast("long"));

        dataframe.show(30);

        Instant end = Instant.now();

        log.info("Query completed in " + Duration.between(start, end).toMillis() + "ms");

        return Duration.between(start, end).toMillis();
    }

}
