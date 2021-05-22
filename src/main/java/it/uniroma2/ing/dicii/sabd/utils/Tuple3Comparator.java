package it.uniroma2.ing.dicii.sabd.utils;


import scala.Tuple3;

import java.io.Serializable;
import java.util.Comparator;

public class Tuple3Comparator<T1, T2, T3> implements Comparator<Tuple3<T1, T2, T3>>, Serializable {

    private final Comparator<T1> comparatorT1;
    private final Comparator<T2> comparatorT2;
    private final Comparator<T3> comparatorT3;

    public Tuple3Comparator(Comparator<T1> comparatorT1, Comparator<T2> comparatorT2, Comparator<T3> comparatorT3) {
        this.comparatorT1 = comparatorT1;
        this.comparatorT2 = comparatorT2;
        this.comparatorT3 = comparatorT3;
    }

    @Override
    public int compare(Tuple3<T1, T2, T3> o1, Tuple3<T1, T2, T3> o2) {
        int result = this.comparatorT1.compare(o1._1(), o2._1());
        if (result == 0) {
            result = comparatorT2.compare(o1._2(), o2._2());
            if(result == 0){
                result = comparatorT3.compare(o1._3(), o2._3());
            }
        }
        return result;
    }

}
