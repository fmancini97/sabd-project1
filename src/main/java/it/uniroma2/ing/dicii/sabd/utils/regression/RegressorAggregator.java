package it.uniroma2.ing.dicii.sabd.utils.regression;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;
import scala.Tuple2;

/**
 * Describes how to perform regression starting from (date-long) pairs.
 *
 * */
public class RegressorAggregator extends Aggregator<Tuple2<Long, Long>, SimpleRegressionWrapper, LineParameters> {

    //Zero value for the regression, it should satisfy a+zero=a
    public SimpleRegressionWrapper zero(){
        return new SimpleRegressionWrapper();
    }

    public SimpleRegressionWrapper reduce(SimpleRegressionWrapper simpleRegression, Tuple2<Long, Long> xy){
        double x = (double)xy._1;
        double y = (double)xy._2;
        simpleRegression.addData(x,y);
        return simpleRegression;
    }

    public SimpleRegressionWrapper merge(SimpleRegressionWrapper a, SimpleRegressionWrapper b){
        a.append(b);
        return a;
    }

    public LineParameters finish(SimpleRegressionWrapper simpleRegression){
        return new LineParameters(simpleRegression.getSlope(), simpleRegression.getIntercept(), simpleRegression.getCounter());
    }

    public Encoder<SimpleRegressionWrapper> bufferEncoder(){
        return Encoders.javaSerialization(SimpleRegressionWrapper.class);
    }

    public Encoder<LineParameters> outputEncoder(){
        return Encoders.bean(LineParameters.class);
    }

}