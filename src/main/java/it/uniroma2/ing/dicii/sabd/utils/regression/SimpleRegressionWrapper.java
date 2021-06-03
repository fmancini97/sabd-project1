package it.uniroma2.ing.dicii.sabd.utils.regression;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.io.Serializable;

/**
 * Wrapper for org.apache.commons.math3.stat.regression.SimpleRegression
 * Maintains the number of points added to the model.
 *
 * */
public class SimpleRegressionWrapper extends SimpleRegression implements Serializable {
    private int counter;

    public SimpleRegressionWrapper(){
        super();
        counter = 0;
    }

    public SimpleRegressionWrapper(int counter){
        super();
        this.counter = counter;
    }

    public int getCounter(){
        return counter;
    }

    public void setCounter(int counter){
        this.counter = counter;
    }

    public void addData(double x, double y){
        super.addData(x,y);
        if(y>0)
            counter++;
    }

    public void append(SimpleRegressionWrapper simpleRegression){
        super.append(simpleRegression);
        counter = counter + simpleRegression.getCounter();
    }

}
