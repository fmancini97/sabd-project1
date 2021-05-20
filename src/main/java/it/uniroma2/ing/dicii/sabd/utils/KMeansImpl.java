package it.uniroma2.ing.dicii.sabd.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;

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
        if (this.model != null) return model.predict(point);
        else return -1;
    }
}
