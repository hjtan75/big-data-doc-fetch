package uk.ac.gla.dcs.bigdata.studentfunctions.filter;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.TotalQueryWordsAccumulator;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWordsDic;

import java.util.*;

public class PreprocessFlatMap implements FlatMapFunction<NewsArticle, ArticleWordsDic> {

    private static final long serialVersionUID = 1938637531715783780L;
    private transient TextPreProcessor processor;

    Broadcast<Set<String>> broadcastQueryWords;

    LongAccumulator wordCountAccumulator;
    LongAccumulator docCountAccumulator;

    TotalQueryWordsAccumulator totalQueryWordsAccumulator;

    public PreprocessFlatMap(LongAccumulator wordCountAccumulator, LongAccumulator docCountAccumulator, TotalQueryWordsAccumulator totalQueryWordsAccumulator, Broadcast<Set<String>> broadcastQueryWords) {
        this.wordCountAccumulator = wordCountAccumulator;
        this.docCountAccumulator = docCountAccumulator;
        this.totalQueryWordsAccumulator = totalQueryWordsAccumulator;
        this.broadcastQueryWords = broadcastQueryWords;
    }

    @Override
    public Iterator<ArticleWordsDic> call(NewsArticle newsArticle) throws Exception {
        // Ignore articles that doesn't contain title or ID
        if (newsArticle.getTitle() == null || newsArticle.getId() == null){
            List<ArticleWordsDic> articleWordsDicList  = new ArrayList<ArticleWordsDic>(0);
            return articleWordsDicList.iterator();
        }

        // Create a list to store processed terms
        // Process title of article and append it to list
        if (processor==null) processor = new TextPreProcessor();
        ArrayList<String> list = new ArrayList<String>();
        if (newsArticle.getTitle() != null){
            list.addAll(processor.process(newsArticle.getTitle()));
        }

        // Process contentItem that has subtype equals to "paragraph"
        int number = 0;
        Iterator<ContentItem> iterator = newsArticle.getContents().iterator();
        while(iterator.hasNext() && number < 5){
            ContentItem content = iterator.next();
            if (content != null){
                if (content.getSubtype()!= null && content.getSubtype().equals("paragraph")){
                    if (content.getContent() != null){
                        list.addAll(processor.process(content.getContent()));
                        number++;
                    }
                }
            }
        }
        wordCountAccumulator.add(list.size());
        docCountAccumulator.add(1);
        
        // Convert list of terms to bag of words
        // Only consider terms that exist in the query
        Set<String> queryWords = broadcastQueryWords.value();
        ArticleWordsDic articleWordsDic = new ArticleWordsDic(newsArticle.getId(), newsArticle.getTitle(), list.size(), new HashMap<String, Integer>());
        Map<String, Integer> mapping = articleWordsDic.getMap();
        for (String s : list){
            if (queryWords.contains(s)){
                if (mapping.containsKey(s)){
                    mapping.put(s, mapping.get(s) + 1);
                }else{
                    mapping.put(s, 1);
                }
            }
        }
        if (mapping.size() == 0){
            List<ArticleWordsDic> articleWordsDicList  = new ArrayList<ArticleWordsDic>(0);
            return articleWordsDicList.iterator();
        }
        totalQueryWordsAccumulator.add(new HashMap<>(mapping));

        List<ArticleWordsDic> articleWordsDicList  = new ArrayList<ArticleWordsDic>(1);
        articleWordsDicList.add(articleWordsDic);
        return articleWordsDicList.iterator();

    }
}
