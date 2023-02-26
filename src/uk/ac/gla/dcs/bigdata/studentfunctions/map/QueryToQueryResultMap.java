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


        //1. Score each article
        // For each query terms, we'll see whether it exists in the document
        // If so add the score the that document.
        for (ArticleWordsDic articleWordsDic : articleWordsList){
            double currentScore = 0;
            Map<String, Integer> mapping = articleWordsDic.getMap();
            for (String term : query.getQueryTerms()){
                int currTotalTermFrequencyInCorpus = totalTermFrequencyInCorpusDic.containsKey(term) ? totalTermFrequencyInCorpusDic.get(term) : 0;
                if (mapping.containsKey(term)){
                    /**
                     * Calculates the DPH score for a single query term in a document
                     * @param termFrequencyInCurrentDocument // The number of times the query appears in the document
                     * @param totalTermFrequencyInCorpus // the number of times the query appears in all documents
                     * @param currentDocumentLength // the length of the current document (number of terms in the document)
                     * @param averageDocumentLengthInCorpus // the average length across all documents
                     * @param totalDocsInCorpus // the number of documents in the corpus
                     * @return
                     */
                    currentScore += DPHScorer.getDPHScore(mapping.get(term).shortValue(), currTotalTermFrequencyInCorpus, articleWordsDic.getLength(), averageDocumentLengthInCorpus, totalDocsInCorpus);
                }
            }
            currentScore = currentScore / query.getQueryTerms().size();
            DPHResult dphScorer = new DPHResult(articleWordsDic.getId(),articleWordsDic.getTitle(), currentScore);
            dphResultList.add(dphScorer);
        }
        
        //2. Sort article based on DPH score
        Collections.sort(dphResultList, new Comparator<DPHResult>() {
            @Override
            public int compare(DPHResult d1, DPHResult d2) {
                if (d1.getScore() == d2.getScore()){
                    return 0;
                }
                return d1.getScore() > d2.getScore() ? -1 : 1;
            }
        });


        //3. Create a final list to hold the top ten document
        // Distance between new document and every documents in the list is calculated
        // If distance > 0.5 document is added, else document is ignore
        // Document is added until the final list has size of 10
        QueryResultWithArticleId queryResultWithArticleId = new QueryResultWithArticleId(query, new ArrayList<>());
        List<DPHResult> finalResultList = queryResultWithArticleId.getArticleIdList();
        for (int i = 0; i < dphResultList.size() && finalResultList.size() < 10; i++) {
            var curr = dphResultList.get(i);
            if (finalResultList.size() == 0){
                finalResultList.add(curr);
                continue;
            }
            boolean add = true;
            for (DPHResult result : finalResultList){
                double score = TextDistanceCalculator.similarity(result.getTitle(), curr.getTitle());
                if (score < 0.5){
                    add = false;
                    break;
                }
            }
            if (add){
                finalResultList.add(dphResultList.get(i));
            }
        }

        return queryResultWithArticleId;
    }
}
