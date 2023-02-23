package uk.ac.gla.dcs.bigdata.studentfunctions.flatmap;

import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.util.Iterator;

public class QueryWordsFlatMap implements FlatMapFunction<Query, String> {
    @Override
    public Iterator<String> call(Query query) throws Exception {
        return query.getQueryTerms().iterator();
    }
}
