package it.uniroma2.ing.dicii.sabd.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;

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
        if (this.model != null) return model.predict(point);
        else return -1;
    }
}
