package it.uniroma2.ing.dicii.sabd.queries;

import it.uniroma2.ing.dicii.sabd.utils.io.HdfsIO;

public interface Query {

    void configure(QueryContext queryContext, HdfsIO hdfsIO);
    Long execute();
}
