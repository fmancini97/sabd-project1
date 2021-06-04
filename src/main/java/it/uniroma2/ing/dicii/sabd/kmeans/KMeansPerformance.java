package it.uniroma2.ing.dicii.sabd.kmeans;

import it.uniroma2.ing.dicii.sabd.utils.io.CSVAble;

/**
 * Maintains K-Means performance metrics.
 *
 * */
public class KMeansPerformance implements CSVAble {
    private KMeansType kMeansType;
    private Integer k;
    private Long trainingCost;
    private Double wssse;


    public KMeansPerformance(KMeansType kMeansType, Integer k, Long trainingCost, Double wssse) {
        this.kMeansType = kMeansType;
        this.k = k;
        this.trainingCost = trainingCost;
        this.wssse = wssse;
    }


    public KMeansType getkMeansType() {
        return kMeansType;
    }

    public void setkMeansType(KMeansType kMeansType) {
        this.kMeansType = kMeansType;
    }

    public Integer getK() {
        return k;
    }

    public void setK(Integer k) {
        this.k = k;
    }

    public Long getTrainingCost() {
        return trainingCost;
    }

    public void setTrainingCost(Long trainingCost) {
        this.trainingCost = trainingCost;
    }

    public Double getWssse() {
        return wssse;
    }

    public void setWssse(Double wssse) {
        this.wssse = wssse;
    }


    @Override
    public String toCSV() {
        return this.kMeansType.toString() + "," + this.k + "," + this.trainingCost + "," + this.wssse;
    }

    @Override
    public String getHeader() {
        return "algoritmo,k,costo addestramento (ms), wssse";
    }
}
