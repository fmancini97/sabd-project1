package it.uniroma2.ing.dicii.sabd.utils.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public class HdfsIO{
    private static final String projectDir = "/sabd";
    private static final String inputDir = projectDir + "/input";
    private static final String outputDir = projectDir + "/output";

    private final SparkSession sparkSession;
    private final String hdfsUrl;
    private final FileSystem hdfs;


    public HdfsIO(SparkSession sparkSession, String hdfsUrl, FileSystem hdfs) {
        this.sparkSession = sparkSession;
        this.hdfsUrl = hdfsUrl;
        this.hdfs = hdfs;
    }

    public void saveRDDasCSV(JavaRDD<Row> rows, StructType structType, String filename) {
        Dataset<Row> dataset = this.sparkSession.createDataFrame(rows, structType);
        dataset.write()
                .format("csv")
                .option("header", true)
                .mode(SaveMode.Overwrite)
                .save(this.hdfsUrl + outputDir + "/" + filename );
    }

    public void saveRDDAsCSV(JavaRDD<Row> rows, List<Tuple3<String, DataType, Boolean>> header, String filename) {
        List<StructField> fields = new ArrayList<>();
        for (Tuple3<String, DataType, Boolean> element: header) {
            fields.add(DataTypes.createStructField(element._1(), element._2(), element._3()));
        }
        StructType structType = DataTypes.createStructType(fields);
        this.saveRDDasCSV(rows, structType, filename);
    }

    public void saveStructAsCSV(List<? extends CSVAble> struct, String filename) throws IOException {
        Path hdfsWritePath = new Path(outputDir + "/" + filename);
        FSDataOutputStream fsDataOutputStream = this.hdfs.create(hdfsWritePath,true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        bufferedWriter.write(struct.get(0).getHeader());
        for (CSVAble line: struct) {
            bufferedWriter.newLine();
            bufferedWriter.write(line.toCSV());
        }
        bufferedWriter.newLine();
        bufferedWriter.close();
    }

    public Dataset<Row> readParquetAsDataframe(String filename){
        return this.sparkSession.read().parquet(this.hdfsUrl + inputDir + "/" + filename);
    }

    public JavaRDD<Row> readParquetAsRDD(String filename) {
        Dataset<Row> dataset = this.readParquetAsDataframe(filename);
        return dataset.toJavaRDD();
    }


    public static HdfsIO createInstance(SparkSession sparkSession, String hdfsUrl) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsUrl);
        FileSystem hdfs = FileSystem.get(configuration);
        return new HdfsIO(sparkSession, hdfsUrl, hdfs);
    }
}
