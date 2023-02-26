package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.DPHResult;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryResultWithArticleId;

import java.util.ArrayList;
import java.util.Map;

public class QueryWithArticleIdToDR implements MapFunction<QueryResultWithArticleId, DocumentRanking> {

    Broadcast<Map<String, NewsArticle>> mapBroadcast;

    public QueryWithArticleIdToDR(Broadcast<Map<String, NewsArticle>> mapBroadcast) {
        this.mapBroadcast = mapBroadcast;
    }

    public QueryWithArticleIdToDR() {
    }

    @Override
    public DocumentRanking call(QueryResultWithArticleId queryResultWithArticleId) throws Exception {
        var map = mapBroadcast.value();

        ArrayList<RankedResult> rankResultList = new ArrayList<>();
        for (DPHResult r : queryResultWithArticleId.getArticleIdList()){
            var rankResult = new RankedResult(r.getId(), map.get(r.getId()), r.getScore());
            rankResultList.add(rankResult);
        }
        var documentRanking = new DocumentRanking(queryResultWithArticleId.getQuery(), rankResultList);
        return documentRanking;
    }
}
