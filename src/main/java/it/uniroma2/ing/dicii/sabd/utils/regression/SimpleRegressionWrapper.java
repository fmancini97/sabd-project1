package it.uniroma2.ing.dicii.sabd.utils.regression;

import org.apache.commons.math3.stat.regression.SimpleRegression;

public class SimpleRegressionWrapper extends SimpleRegression {

    private int counter;

    public SimpleRegressionWrapper(){
        super();
        counter = 0;
    }

    public int getCounter(){
        return counter;
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
