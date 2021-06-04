package it.uniroma2.ing.dicii.sabd.queries;

import it.uniroma2.ing.dicii.sabd.utils.io.CSVAble;

/**
 * Holds information about query performance
 *
 * */
public class QueryPerformance implements CSVAble {
    private QueryType queryType;
    private Long queryTime;

    public QueryPerformance(QueryType queryType, Long queryTime) {
        this.queryType = queryType;
        this.queryTime = queryTime;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    public Long getQueryTime() {
        return queryTime;
    }

    public void setQueryTime(Long queryTime) {
        this.queryTime = queryTime;
    }


    @Override
    public String toCSV() {
        return this.queryType.toString() + "," + this.queryTime;
    }

    @Override
    public String getHeader() {
        return "query,runtime (ms)";
    }
}
