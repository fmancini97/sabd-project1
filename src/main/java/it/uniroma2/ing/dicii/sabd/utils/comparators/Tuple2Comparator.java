package it.uniroma2.ing.dicii.sabd.utils.comparators;

import scala.Tuple2;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Compares scala.Tuple2 objects
 *
 * */
public class Tuple2Comparator<K,V> implements Comparator<Tuple2<K, V>>, Serializable {

    private final Comparator<K> comparatorK;
    private final Comparator<V> comparatorV;

    public Tuple2Comparator(Comparator<K> comparatorK, Comparator<V> comparatorV) {
        this.comparatorK = comparatorK;
        this.comparatorV = comparatorV;
    }

    @Override
    public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
        int result = this.comparatorK.compare(o1._1, o2._1);
        if (result == 0) return this.comparatorV.compare(o1._2, o2._2);
        else return result;
    }

}