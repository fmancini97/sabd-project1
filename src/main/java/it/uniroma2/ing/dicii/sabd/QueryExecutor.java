package it.uniroma2.ing.dicii.sabd;

import it.uniroma2.ing.dicii.sabd.queries.QueryPerformance;
import it.uniroma2.ing.dicii.sabd.queries.QueryContext;
import it.uniroma2.ing.dicii.sabd.queries.QueryType;
import it.uniroma2.ing.dicii.sabd.utils.io.HdfsIO;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for query execution.
 *
 * */
public class QueryExecutor {

    public static void main(String[] args) {
        Logger log = LogManager.getLogger("SABD Project");
        List<QueryPerformance> queryPerformances = new ArrayList<>();
        HdfsIO hdfsIO = null;
        SparkSession spark = SparkSession
                .builder()
                .appName("SABD Project 1")
                .config("spark.sql.shuffle.partitions", 20)
                .getOrCreate();
        QueryContext queryContext = new QueryContext(spark);

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
                Method method = cls.getMethod("execute", QueryContext.class, HdfsIO.class);
                Long executionTime = (Long) method.invoke(null, queryContext, hdfsIO);
                queryPerformances.add(new QueryPerformance(queryType, executionTime));
            } catch (ClassNotFoundException e) {
                log.error("Class not found: " + e.getMessage());
            } catch ( NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                e.printStackTrace();
                log.error(e.getMessage());
            }
        }

        try {
            hdfsIO.saveStructAsCSV(queryPerformances, "queriesPerformance.csv");
        } catch (IOException e) {
            log.error("Error while saving queries benchmark: " + e.getMessage());
        }

        log.info("Query processing concluded");
        spark.close();
    }
}
