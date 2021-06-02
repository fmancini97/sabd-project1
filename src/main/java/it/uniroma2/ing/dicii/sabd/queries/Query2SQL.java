package it.uniroma2.ing.dicii.sabd.queries;

import it.uniroma2.ing.dicii.sabd.utils.io.HdfsIO;
import it.uniroma2.ing.dicii.sabd.utils.regression.LineParameters;
import it.uniroma2.ing.dicii.sabd.utils.regression.SimpleRegressionWrapper;
import it.uniroma2.ing.dicii.sabd.utils.regression.XY;
import javassist.Loader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.UserDefinedAggregator;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Array;
import scala.Option;
import scala.Serializable;
import scala.Tuple2;

import java.sql.Struct;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class Query2SQL implements Query {

    private final Logger log;
    private static final String vaccineAdministrationFile = "somministrazioni-vaccini-latest.parquet";
    private static final String resultFile = "query2Result";
    private static final String dateFirstFeb2021String = "2021-02-01";
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
    private QueryContext queryContext;
    private HdfsIO hdfsIO;
    private SparkSession sparkSession;

    public Query2SQL() {
        this.log = LogManager.getLogger(getClass().getSimpleName());
    }

    @Override
    public void configure(QueryContext queryContext, HdfsIO hdfsIO) {
        this.queryContext = queryContext;
        this.hdfsIO = hdfsIO;
        this.sparkSession = queryContext.getSparkSession();
    }

    @Override
    public Long execute() {

        log.info("Starting processing query");
        Instant start = Instant.now();

        Dataset<Row> dataframe = this.hdfsIO.readParquetAsDataframe(vaccineAdministrationFile);

        dataframe = dataframe.withColumn("data_somministrazione", to_date(dataframe.col("data_somministrazione")));
        dataframe = dataframe.withColumn("sesso_femminile", dataframe.col("sesso_femminile").cast("long"));

        dataframe = dataframe.filter(dataframe.col("data_somministrazione").geq(lit(dateFirstFeb2021String)));

        dataframe.createOrReplaceTempView("table");

        dataframe = sparkSession.sql("SELECT data_somministrazione, fascia_anagrafica, " +
                "sum(sesso_femminile) as sesso_femminile,nome_area FROM table" +
                " GROUP BY data_somministrazione, fascia_anagrafica, nome_area");

        dataframe = dataframe.withColumn("anno_mese", functions.concat(
                functions.month(dataframe.col("data_somministrazione")), lit("-"),
                functions.year(dataframe.col("data_somministrazione"))));

        dataframe = dataframe.withColumn("data_somministrazione", functions.unix_timestamp(dataframe.col("data_somministrazione")));

        dataframe = dataframe.withColumn("xy", functions.struct(dataframe.col("data_somministrazione"), dataframe.col("sesso_femminile")));

        dataframe = dataframe.groupBy("anno_mese", "fascia_anagrafica", "nome_area").agg(functions.collect_list("xy"));

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("anno_mese", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("fascia_anagrafica", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("nome_area", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("vaccinazioni_predette", DataTypes.LongType, true));
        StructType rowSchema = DataTypes.createStructType(fields);
/*
        dataframe = dataframe.map((MapFunction<Row,Row>)
                row -> {

            int indexFasciaAnagrafica = row.fieldIndex("fascia_anagrafica");
            int indexNomeArea = row.fieldIndex("nome_area");
            int indexAnnoMese = row.fieldIndex("anno_mese");

            List<Row> xyList = row.getList(3);
            SimpleRegressionWrapper simpleRegressionWrapper = new SimpleRegressionWrapper();

            int size = xyList.size();
            double[][] xyMatrix = new double[size][2];

            for(int i = 0; i < size; i++){
                Row xy = xyList.get(i);
                xyMatrix[i][0] = (double)(Long)xy.get(0);
                xyMatrix[i][1] = (double)(Long)xy.get(1);
            }

            simpleRegressionWrapper.addData(xyMatrix);

            for(Row xy:xyList){
                simpleRegressionWrapper.addData((double)xy.getLong(0), (double)xy.getLong(1));
            }
            String yearMonth = row.getString(indexAnnoMese);

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(simpleDateFormat.parse("01-0"+yearMonth));
            calendar.add(Calendar.MONTH,1);

            String firstDayNextMonth = simpleDateFormat.format(calendar.getTime());

            double valuePredicted = simpleRegressionWrapper.predict((double)calendar.getTime().getTime()/1000);


            return RowFactory.create(firstDayNextMonth, row.getString(indexFasciaAnagrafica),
                    row.getString(indexNomeArea), Math.round(valuePredicted));
        }, RowEncoder.apply(rowSchema));
*/


        /*
        List<StructField> xyFields = new ArrayList<>();
        xyFields.add(DataTypes.createStructField("x", DataTypes.DoubleType,false));
        xyFields.add(DataTypes.createStructField("y", DataTypes.DoubleType,false));

        UserDefinedFunction toXY = udf(
                (Long x, Long  y) -> {return new XY(x,y);}, Tuple2.class);

        sparkSession.udf().register("toXY", functions.udf(
                (Long x, Long  y) -> {return new XY(x,y);}, DataTypes.createStructType(xyFields)));


        dataframe.show(10);
        dataframe.printSchema();

        sparkSession.udf().register("linearRegression", functions.udaf(new RegressorAggregator(),
                Encoders.tuple(Encoders.LONG(), Encoders.LONG())));

        dataframe.createOrReplaceTempView("table");
        dataframe.schema().fieldNames();

        dataframe = sparkSession.sql("SELECT anno_mese, fascia_anagrafica, nome_area, " +
                "linearRegression(data_somministrazione, sesso_femminile) FROM table " +
                "GROUP BY anno_mese, fascia_anagrafica, nome_area " +
                "ORDER BY anno_mese, fascia_anagrafica, nome_area");
 */

    //    dataframe.createOrReplaceTempView("table");
    //    dataframe = sparkSession.sql("SELECT * FROM table SORT BY anno_mese, fascia_anagrafica, nome_area");

      //  dataframe = dataframe.udaf



    //    dataframe.printSchema();
        dataframe.show(false);


        Instant end = Instant.now();

        log.info("Query completed in " + Duration.between(start, end).toMillis() + "ms");

        return Duration.between(start, end).toMillis();
    }
/*
    public static class RegressorAggregator extends Aggregator<Tuple2<Long, Long>, SimpleRegressionWrapper, LineParameters> {


        //Valore zero per l'aggregazione - dovrebbe soddisfare a+zero=a;
        public SimpleRegressionWrapper zero(){
            return new SimpleRegressionWrapper();
        }

        public SimpleRegressionWrapper reduce(SimpleRegressionWrapper simpleRegression, Tuple2<Long, Long> xy){
            double x = (double)xy._1;
            double y = (double)xy._2;
            simpleRegression.addData(x,y);
            return simpleRegression;
        }

        public SimpleRegressionWrapper merge(SimpleRegressionWrapper a, SimpleRegressionWrapper b){
            Logger log = LogManager.getLogger(getClass().getSimpleName());
            log.error(a.getN() + " " + b.getN());
            a.append(b);
            return a;
        }

        public LineParameters finish(SimpleRegressionWrapper simpleRegression){
            return new LineParameters(simpleRegression.getSlope(), simpleRegression.getIntercept());
        }

        public Encoder<SimpleRegressionWrapper> bufferEncoder(){
            return Encoders.bean(SimpleRegressionWrapper.class);
        }

        public Encoder<LineParameters> outputEncoder(){
            return Encoders.bean(LineParameters.class);
        }

    }
*/
}
