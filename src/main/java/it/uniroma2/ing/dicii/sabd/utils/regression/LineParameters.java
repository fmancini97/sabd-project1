package it.uniroma2.ing.dicii.sabd.utils.regression;

public class LineParameters {

    private double slope;
    private double intercept;

    public LineParameters(double slope, double intercept){
        this.slope = slope;
        this.intercept = intercept;
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



}
