package it.uniroma2.ing.dicii.sabd.queries;


public enum QueryType {
//    QUERY1("Query1"),
//    QUERY2("Query2"),
//    QUERY3("Query3"),
//    QUERY1SQL("Query1SQL"),
    QUERY2SQL("Query2SQL");

    private static final String packageName = QueryType.class.getPackage().getName();
    private final String queryClass;

    QueryType(final String queryClass) {
        this.queryClass = queryClass;
    }

    public String getQueryClass() {
        return packageName + "." + this.queryClass;
    }
}
