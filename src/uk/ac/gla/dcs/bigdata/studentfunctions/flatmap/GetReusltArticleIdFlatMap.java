package uk.ac.gla.dcs.bigdata.studentfunctions.flatmap;

import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.DPHResult;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryResultWithArticleId;

import java.util.ArrayList;
import java.util.Iterator;

public class GetReusltArticleIdFlatMap implements FlatMapFunction<QueryResultWithArticleId, String> {

    @Override
    public Iterator<String> call(QueryResultWithArticleId queryResultWithArticleId) throws Exception {
        var list = new ArrayList<String>();
        for (DPHResult dphResult : queryResultWithArticleId.getArticleIdList()){
            list.add(dphResult.getId());
        }
        return list.iterator();
    }
}