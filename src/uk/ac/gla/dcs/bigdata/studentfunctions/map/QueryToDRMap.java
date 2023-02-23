package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWordsDic;
import uk.ac.gla.dcs.bigdata.studentstructures.DPHResult;

import java.util.*;

public class QueryToDRMap implements MapFunction<Query, DocumentRanking> {

    private static final long serialVersionUID = -484410270146328326L;

    Broadcast<List<ArticleWordsDic>> listBroadcast;
    Broadcast<Long> totalDocsInCorpus;
    Broadcast<Double> averageDocumentLengthInCorpus;
    Broadcast<Map<String, Integer>> totalTermFrequencyInCorpusDic;
    Broadcast<Map<String, NewsArticle>> newsArticleMapBroadcast;

    public QueryToDRMap(Broadcast<List<ArticleWordsDic>> listBroadcast, Broadcast<Long> totalDocsInCorpus,
                        Broadcast<Double> averageDocumentLengthInCorpus,
                        Broadcast<Map<String, Integer>> totalTermFrequencyInCorpusDic, Broadcast<Map<String, NewsArticle>> newsArticleMapBroadcast) {
        this.listBroadcast = listBroadcast;
        this.totalDocsInCorpus = totalDocsInCorpus;
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
        this.totalTermFrequencyInCorpusDic = totalTermFrequencyInCorpusDic;
        this.newsArticleMapBroadcast = newsArticleMapBroadcast;
    }

    @Override
    public DocumentRanking call(Query query) throws Exception {
        List<ArticleWordsDic> articleWordsList = listBroadcast.value();
        Long totalDocsInCorpus = this.totalDocsInCorpus.value();
        Double averageDocumentLengthInCorpus = this.averageDocumentLengthInCorpus.value();
        Map<String, Integer> totalTermFrequencyInCorpusDic = this.totalTermFrequencyInCorpusDic.value();
        Map<String, NewsArticle> newsArticleMap = this.newsArticleMapBroadcast.value();

        List<DPHResult> dphResultList = new ArrayList<>();


        //1. get the score of articles
        for (ArticleWordsDic articleWordsDic : articleWordsList){
            double currentScore = 0;
            HashMap<String, Integer> mapping = articleWordsDic.getMapping();
            for (String term : query.getQueryTerms()){
                int currTotalTermFrequencyInCorpus = totalTermFrequencyInCorpusDic.containsKey(term) ? totalTermFrequencyInCorpusDic.get(term) : 0;
                if (!mapping.containsKey(term)){
                    currentScore += DPHScorer.getDPHScore((short)0, currTotalTermFrequencyInCorpus, articleWordsDic.getLength(), averageDocumentLengthInCorpus, totalDocsInCorpus);
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
                return d1.getScore() > d2.getScore() ? -1 : 1;
            }
        });
        //3. calculate distance and generate List<RankedResult>
        List<RankedResult> rankedResultList= new ArrayList<>();
        for (int i = 0; i < dphResultList.size() && rankedResultList.size() <= 10; i++) {
            var curr = dphResultList.get(i);
            if (rankedResultList.size() == 0){
                var randedResult = new RankedResult(curr.getId(), newsArticleMap.get(curr.getId()),curr.getScore());
                rankedResultList.add(randedResult);
                continue;
            }
            double score = TextDistanceCalculator.similarity(
                    dphResultList.get(dphResultList.size() - 1).getTitle(), dphResultList.get(i+1).getTitle());
            if (score >= 0.5){
                var randedResult = new RankedResult(curr.getId(), newsArticleMap.get(curr.getId()),curr.getScore());
                rankedResultList.add(randedResult);
            }
        }

        return new DocumentRanking(query, rankedResultList);
    }
}
