package it.uniroma2.ing.dicii.sabd.queries;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Date;

/**
 * Holds information about execution environment
 *
 * */
public class QueryContext {
    private JavaPairRDD<Date, Tuple2<String, Long>> vaccineAdministrationSummary;
    private final SparkSession sparkSession;

    public SparkSession getSparkSession(){
        return this.sparkSession;
    }

    public QueryContext(SparkSession sparkSession) {
        this.vaccineAdministrationSummary = null;
        this.sparkSession = sparkSession;
    }

    public JavaPairRDD<Date, Tuple2<String, Long>> cacheVaccineAdministration(JavaPairRDD<Date, Tuple2<String, Long>> vaccineAdministration) {
        this.vaccineAdministrationSummary = vaccineAdministration.cache();
        return this.vaccineAdministrationSummary;
    }

    public JavaPairRDD<Date, Tuple2<String, Long>> getVaccineAdministrationSummary() {
        return vaccineAdministrationSummary;
    }
}
