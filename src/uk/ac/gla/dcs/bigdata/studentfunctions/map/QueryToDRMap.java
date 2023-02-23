package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWordsDic;
import uk.ac.gla.dcs.bigdata.studentstructures.DPHResult;

import java.util.*;

public class QueryToDRMap implements MapFunction<Query, DocumentRanking> {

    private static final long serialVersionUID = -484410270146328326L;

    Broadcast<List<ArticleWordsDic>> listBroadcast;
    Broadcast<Long> totalDocsInCorpus;
    Broadcast<Double> averageDocumentLengthInCorpus;
    Broadcast<Map<String, Integer>> totalTermFrequencyInCorpusDic;

    public QueryToDRMap(Broadcast<List<ArticleWordsDic>> listBroadcast, Broadcast<Long> totalDocsInCorpus,
                        Broadcast<Double> averageDocumentLengthInCorpus,
                        Broadcast<Map<String, Integer>> totalTermFrequencyInCorpusDic) {
        this.listBroadcast = listBroadcast;
        this.totalDocsInCorpus = totalDocsInCorpus;
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
        this.totalTermFrequencyInCorpusDic = totalTermFrequencyInCorpusDic;
    }

    @Override
    public DocumentRanking call(Query query) throws Exception {
        var articleWordsList = listBroadcast.value();
        var totalDocsInCorpus = this.totalDocsInCorpus.value();
        var averageDocumentLengthInCorpus = this.averageDocumentLengthInCorpus.value();
        var totalTermFrequencyInCorpusDic = this.totalTermFrequencyInCorpusDic.value();

        List<DPHResult> dphResultList = new ArrayList<>();


        //1. get the score of articles
        for (ArticleWordsDic articleWordsDic : articleWordsList){
            double currentScore = 0;
            var mapping = articleWordsDic.getMapping();
            for (String term : query.getQueryTerms()){
                var currTotalTermFrequencyInCorpus = totalTermFrequencyInCorpusDic.containsKey(term) ? totalTermFrequencyInCorpusDic.get(term) : 0;
                if (!mapping.containsKey(term)){
                    currentScore += DPHScorer.getDPHScore((short)0, currTotalTermFrequencyInCorpus, articleWordsDic.getLength(), averageDocumentLengthInCorpus, totalDocsInCorpus);
                }
            }
            currentScore = currentScore / query.getQueryTerms().size();
            DPHResult dphScorer = new DPHResult(articleWordsDic.getId(), currentScore);
            dphResultList.add(dphScorer);
        }
        //2. sort by the score
        Collections.sort(dphResultList, new Comparator<DPHResult>() {
            @Override
            public int compare(DPHResult d1, DPHResult d2) {
                return d1.getScore() > d2.getScore() ? -1 : 1;
            }
        });
        //3.

        return null;
    }
}
