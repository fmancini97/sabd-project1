package it.uniroma2.ing.dicii.sabd.utils.regression;

import java.io.Serializable;

/**
 * Maintains information about regression line.
 * Slope: slope of the regression line
 * Intercept: intercept of the regression line
 * Counter: number of points used to fit the regression line
 *
 * */
public class LineParameters implements Serializable {

    private double slope;
    private double intercept;
    private int counter;

    public LineParameters() {}

    public LineParameters(double slope, double intercept, int counter){
        this.slope = slope;
        this.intercept = intercept;
        this.counter = counter;
    }

    public double getSlope() {
        return slope;
    }

    public void setSlope(double slope) {
        this.slope = slope;
    }

    public double getIntercept() {
        return intercept;
    }

    public void setIntercept(double intercept) {
        this.intercept = intercept;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }
}
