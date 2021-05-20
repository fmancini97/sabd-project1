package it.uniroma2.ing.dicii.sabd.utils;

import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;

public abstract class KMeansAlgorithm implements Serializable {

    public abstract void train(JavaRDD<Vector> dataset, Integer k, Integer numIterations);
    public abstract Integer predict(Vector point);

}
