package it.uniroma2.ing.dicii.sabd;

import it.uniroma2.ing.dicii.sabd.queries.Query;
import it.uniroma2.ing.dicii.sabd.queries.QueryBenchmark;
import it.uniroma2.ing.dicii.sabd.queries.QueryContext;
import it.uniroma2.ing.dicii.sabd.queries.QueryType;
import it.uniroma2.ing.dicii.sabd.utils.io.HdfsIO;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class QueryExecutor {

    public static void main(String[] args) {

        Logger log = LogManager.getLogger("SABD Project");
        List<QueryBenchmark> queryBenchmarks = new ArrayList<>();
        HdfsIO hdfsIO = null;
        QueryContext queryContext = new QueryContext();

        SparkSession spark = SparkSession
                .builder()
                .appName("SABD Project 1")
                .getOrCreate();

        try {
            hdfsIO = HdfsIO.createInstance(spark, "hdfs://hdfs-master:54310");
        } catch (IOException e) {
            log.error("Error while establishing connection to HDFS: " + e.getMessage());
            System.exit(1);
        }

        log.info("Starting processing queries");
        for (QueryType queryType: QueryType.values()) {
            try {
                Class<?> cls = Class.forName(queryType.getQueryClass());
                Constructor<?> constructor = cls.getConstructor();
                Query query = (Query) constructor.newInstance();
                query.configure(queryContext, hdfsIO);
                Long executionTime = query.execute();
                queryBenchmarks.add(new QueryBenchmark(queryType, executionTime));
            } catch (ClassNotFoundException e) {
                log.error("Class not found: " + e.getMessage());
            } catch ( NoSuchMethodException | InvocationTargetException
                    | InstantiationException | IllegalAccessException e) {
                log.error(e.getMessage());
            }
        }

        try {
            hdfsIO.saveStructAsCSV(queryBenchmarks, "queriesBenchmark.csv");
        } catch (IOException e) {
            log.error("Error while saving queries benchmark: " + e.getMessage());
        }

        log.info("Query processing concluded");
        spark.close();
    }
}
