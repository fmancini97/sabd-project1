package it.uniroma2.ing.dicii.sabd.kmeans;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;

/**
 * Implementation of K-Means
 *
 * */
public class KMeansImpl extends KMeansAlgorithm implements Serializable {
    private KMeansModel model;

    public KMeansImpl() {
        this.model = null;
    }

    @Override
    public void train(JavaRDD<Vector> dataset, Integer k, Integer numIterations) {
        this.model = KMeans.train(dataset.rdd(), k, numIterations);
    }

    @Override
    public Integer predict(Vector point) {
        if (this.model != null) return this.model.predict(point);
        else return -1;
    }

    @Override
    public Double trainingCost() {
        if (this.model != null) return this.model.trainingCost();
        else return -1.0;
    }

    @Override
    public Double computeCost(JavaRDD<Vector> dataset) {
        if (this.model != null) return this.model.computeCost(dataset.rdd());
        else return -1.0;
    }

}
