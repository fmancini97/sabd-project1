package it.uniroma2.ing.dicii.sabd.utils;

public enum Algorithm {
    KMEANS("KMeansImpl"),
    BISECTINGKMEANS("BisectingKMeansImpl");

    private final String algorithmClass;
    private Algorithm(final String algorithmClass) {
        this.algorithmClass = algorithmClass;
    }
    public String getAlgorithmClass() {
        return "it.uniroma2.ing.dicii.sabd.utils." + this.algorithmClass;
    }
}