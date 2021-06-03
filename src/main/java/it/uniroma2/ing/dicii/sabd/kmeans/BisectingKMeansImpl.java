package it.uniroma2.ing.dicii.sabd.kmeans;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;

/**
 * Implementation of Bisecting K-Means
 *
 * */
public class BisectingKMeansImpl extends KMeansAlgorithm implements Serializable {
    private BisectingKMeansModel model;

    public BisectingKMeansImpl() {
        this.model = null;
    }

    @Override
    public void train(JavaRDD<Vector> dataset, Integer k, Integer numIterations) {
        BisectingKMeans algorithm = new BisectingKMeans();
        algorithm.setK(k);
        this.model = algorithm.run(dataset);
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
        if (this.model != null) return this.model.computeCost(dataset);
        else return -1.0;
    }
}
