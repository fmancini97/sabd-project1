package it.uniroma2.ing.dicii.sabd.kmeans;

public enum KMeansType {
    KMEANS("KMeansImpl"),
    BISECTINGKMEANS("BisectingKMeansImpl");

    private static final String packageName = KMeansType.class.getPackage().getName();
    private final String algorithmClass;

    private KMeansType(final String algorithmClass) {
        this.algorithmClass = algorithmClass;
    }

    public String getAlgorithmClass() {
        return packageName + "." + this.algorithmClass;
    }
}