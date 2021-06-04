package it.uniroma2.ing.dicii.sabd.queries;

import it.uniroma2.ing.dicii.sabd.utils.io.HdfsIO;
import it.uniroma2.ing.dicii.sabd.utils.regression.RegressorAggregator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.*;
import org.apache.spark.sql.types.DataTypes;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Runs Query1 using Spark SQL
 *
 * */
public class Query2SQL {

    private static final String vaccineAdministrationFile = "somministrazioni-vaccini-latest.parquet";
    private static final String resultFile = "query2SQLResult";
    private static final String dateFirstFeb2021String = "2021-02-01";
    private static final String dateFirstJun2021String = "2021-06-01";
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM");

    /**
     * @param queryContext: object that holds information about sparkSession
     * @param hdfsIO: object that handles IO with HDFS
     * */
    public static Long execute(QueryContext queryContext, HdfsIO hdfsIO) {
        Logger log = LogManager.getLogger(Query2SQL.class.getSimpleName());
        SparkSession sparkSession = queryContext.getSparkSession();

        log.info("Starting processing query");
        Instant start = Instant.now();

        // [k:(data, regione, età), v: vaccini]
        Dataset<Row> dataframe = hdfsIO.readParquetAsDataframe(vaccineAdministrationFile);

        UserDefinedFunction nameParser = udf((String nome_area) -> nome_area.split(" /")[0], DataTypes.StringType);
        dataframe = dataframe.withColumn("nome_area", nameParser.apply(dataframe.col("nome_area")));

        dataframe = dataframe.withColumn("data_somministrazione", to_date(dataframe.col("data_somministrazione")));
        dataframe = dataframe.withColumn("sesso_femminile", dataframe.col("sesso_femminile").cast("long"));

        dataframe = dataframe.filter(dataframe.col("data_somministrazione").geq(lit(dateFirstFeb2021String)));
        dataframe = dataframe.filter(dataframe.col("data_somministrazione").lt(lit(dateFirstJun2021String)));

        dataframe.createOrReplaceTempView("table");
        // [(data, fascia_anagrafica, vaccinazioni, regione)]
        dataframe = sparkSession.sql("SELECT data_somministrazione, fascia_anagrafica, " +
                "sum(sesso_femminile) as sesso_femminile,nome_area FROM table" +
                " GROUP BY data_somministrazione, fascia_anagrafica, nome_area");

        // [(anno_mese, fascia_anagrafica, vaccinazioni, regione, data)]
        dataframe = dataframe.withColumn("anno_mese", functions.concat(
                functions.year(dataframe.col("data_somministrazione")), lit("-"),
                functions.month(dataframe.col("data_somministrazione"))));

        dataframe = dataframe.withColumn("data_somministrazione", functions.unix_timestamp(dataframe.col("data_somministrazione")));

        sparkSession.udf().register("linearRegression", functions.udaf(new RegressorAggregator(),
                Encoders.tuple(Encoders.LONG(), Encoders.LONG())));

        dataframe.createOrReplaceTempView("table");

        // [(anno_mese, fascia_anagrafica, regione, (coefficiente angolare, intercetta))]
        dataframe = sparkSession.sql("SELECT anno_mese, fascia_anagrafica, nome_area, " +
                "linearRegression(data_somministrazione, sesso_femminile) AS retta_regressione " +
                "FROM table GROUP BY anno_mese, fascia_anagrafica, nome_area");

        dataframe.filter(dataframe.col("retta_regressione.counter").geq(2));

        // [(anno_mese, fascia_anagrafica, regione, (prossimo_mese,vaccinazioni_previste))]
        UserDefinedFunction doPrediction = udf((String anno_mese, Row lineparameters) -> {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(simpleDateFormat.parse(anno_mese));
            calendar.add(Calendar.MONTH, 1);
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            long nextmonthMillis = calendar.getTimeInMillis();
            java.sql.Date nextMonth = new java.sql.Date(nextmonthMillis);
            long nextmonthTimestamp = nextmonthMillis / 1000;
            Long predictedValue = (long) (lineparameters.getDouble(lineparameters.fieldIndex("slope"))
                    * nextmonthTimestamp + lineparameters.getDouble(lineparameters.fieldIndex("intercept")));
            return RowFactory.create(nextMonth, predictedValue);
        }, DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("data", DataTypes.DateType, false),
                DataTypes.createStructField("vaccinazioni_previste", DataTypes.LongType, false))));

        dataframe = dataframe.withColumn("previsione", doPrediction.apply(dataframe.col("anno_mese"),
                dataframe.col("retta_regressione")));

        // [(anno_mese, fascia_anagrafica, regione, prossimo_mese,vaccinazioni_previste)]
        dataframe = dataframe.withColumn("data", dataframe.col("previsione").getField("data"))
                .withColumn("vaccinazioni_previste", dataframe.col("previsione").getField("vaccinazioni_previste"));

        dataframe.createOrReplaceTempView("table");
        // For each (data, fascia_anagrafica) performs ranking over "vaccinazioni_previste"
        dataframe = sparkSession.sql("SELECT data, fascia_anagrafica, nome_area, vaccinazioni_previste, RANK() over (PARTITION BY data, fascia_anagrafica order by vaccinazioni_previste DESC) as rank FROM table ORDER BY data, fascia_anagrafica, rank ASC");
        // For each (data, fascia_anagrafica) mantains top5 based on the previously calculated rank
        dataframe = dataframe.select(dataframe.col("data"), dataframe.col("fascia_anagrafica"),
                dataframe.col("nome_area"), dataframe.col("vaccinazioni_previste")).where("rank <= 5");

        //dataframe.show(30, false);

        hdfsIO.saveDataframeAsCSV(dataframe, resultFile);

        Instant end = Instant.now();

        log.info("Query completed in " + Duration.between(start, end).toMillis() + "ms");

        return Duration.between(start, end).toMillis();
    }
}


