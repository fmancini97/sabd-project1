package it.uniroma2.ing.dicii.sabd.kmeans;

import it.uniroma2.ing.dicii.sabd.utils.io.CSVAble;

public class KMeansBenchmark implements CSVAble {
    private KMeansType kMeansType;
    private Integer k;
    private Double trainingCost;
    private Double wssse;


    public KMeansBenchmark(KMeansType kMeansType, Integer k, Double trainingCost, Double wssse) {
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

    public Double getTrainingCost() {
        return trainingCost;
    }

    public void setTrainingCost(Double trainingCost) {
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
        return "algorithm,k,training cost, WSSSE";
    }
}
