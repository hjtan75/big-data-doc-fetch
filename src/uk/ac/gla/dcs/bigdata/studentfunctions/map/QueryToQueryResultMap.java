package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWordsDic;
import uk.ac.gla.dcs.bigdata.studentstructures.DPHResult;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryResultWithArticleId;

import java.util.*;

public class QueryToQueryResultMap implements MapFunction<Query, QueryResultWithArticleId> {

    private static final long serialVersionUID = -484410270146328326L;

    Broadcast<List<ArticleWordsDic>> listBroadcast;
    Broadcast<Long> totalDocsInCorpus;
    Broadcast<Double> averageDocumentLengthInCorpus;
    Broadcast<Map<String, Integer>> totalTermFrequencyInCorpusDic;

    public QueryToQueryResultMap(Broadcast<List<ArticleWordsDic>> listBroadcast, Broadcast<Long> totalDocsInCorpus,
                                 Broadcast<Double> averageDocumentLengthInCorpus,
                                 Broadcast<Map<String, Integer>> totalTermFrequencyInCorpusDic) {
        this.listBroadcast = listBroadcast;
        this.totalDocsInCorpus = totalDocsInCorpus;
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
        this.totalTermFrequencyInCorpusDic = totalTermFrequencyInCorpusDic;
    }

    @Override
    public QueryResultWithArticleId call(Query query) throws Exception {
        List<ArticleWordsDic> articleWordsList = listBroadcast.value();
        Long totalDocsInCorpus = this.totalDocsInCorpus.value();
        Double averageDocumentLengthInCorpus = this.averageDocumentLengthInCorpus.value();
        Map<String, Integer> totalTermFrequencyInCorpusDic = this.totalTermFrequencyInCorpusDic.value();

        List<DPHResult> dphResultList = new ArrayList<>();


        //1. get the score of articles
        for (ArticleWordsDic articleWordsDic : articleWordsList){
            double currentScore = 0;
            Map<String, Integer> mapping = articleWordsDic.getMap();
            for (String term : query.getQueryTerms()){
                int currTotalTermFrequencyInCorpus = totalTermFrequencyInCorpusDic.containsKey(term) ? totalTermFrequencyInCorpusDic.get(term) : 0;
                if (mapping.containsKey(term)){
                    //System.out.println("termFrequencyInCurrentDocument " + mapping.get(term) + " currTotalTermFrequencyInCorpus: " + currTotalTermFrequencyInCorpus + " currentDocLength: " + articleWordsDic.getLength());
                    currentScore += DPHScorer.getDPHScore(mapping.get(term).shortValue(), currTotalTermFrequencyInCorpus, articleWordsDic.getLength(), averageDocumentLengthInCorpus, totalDocsInCorpus);
                }
            }
            currentScore = currentScore / query.getQueryTerms().size();
            DPHResult dphScorer = new DPHResult(articleWordsDic.getId(),articleWordsDic.getTitle(), currentScore);
            dphResultList.add(dphScorer);
        }
        //2. sort by the score
        Collections.sort(dphResultList, new Comparator<DPHResult>() {
            @Override
            public int compare(DPHResult d1, DPHResult d2) {
                if (d1.getScore() == d2.getScore()){
                    return 0;
                }
                return d1.getScore() > d2.getScore() ? -1 : 1;
            }
        });


        //3. calculate distance and generate inter query
        QueryResultWithArticleId queryResultWithArticleId = new QueryResultWithArticleId(query, new ArrayList<>());
        List<DPHResult> list = queryResultWithArticleId.getArticleIdList();
        for (int i = 0; i < dphResultList.size() && list.size() < 10; i++) {
            var curr = dphResultList.get(i);
            if (list.size() == 0){
                list.add(curr);
                continue;
            }
            double score = TextDistanceCalculator.similarity(list.get(list.size() - 1).getTitle(), curr.getTitle());
            if (score >= 0.5){
                list.add(dphResultList.get(i));
            }
        }

        return queryResultWithArticleId;
    }
}
