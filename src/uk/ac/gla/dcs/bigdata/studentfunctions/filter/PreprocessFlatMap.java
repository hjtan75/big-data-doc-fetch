package uk.ac.gla.dcs.bigdata.studentfunctions.filter;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWords;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PreprocessFlatMap implements FlatMapFunction<NewsArticle, ArticleWords> {

    private static final long serialVersionUID = 1938637531715783780L;
    private transient TextPreProcessor processor;

    LongAccumulator wordCountAccumulator;
    LongAccumulator docCountAccumulator;

    public PreprocessFlatMap(LongAccumulator wordCountAccumulator, LongAccumulator docCountAccumulator) {
        this.wordCountAccumulator = wordCountAccumulator;
        this.docCountAccumulator = docCountAccumulator;
    }

    @Override
    public Iterator<ArticleWords> call(NewsArticle newsArticle) throws Exception {
        if (newsArticle.getTitle() == null || newsArticle.getId() == null){
            List<ArticleWords> articleList  = new ArrayList<ArticleWords>(0);
            return articleList.iterator();
        }
        if (processor==null) processor = new TextPreProcessor();
        ArrayList<String> list = new ArrayList<String>();
        if (newsArticle.getTitle() != null){
            list.addAll(processor.process(newsArticle.getTitle()));
        }
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
        ArticleWords articleWords = new ArticleWords(newsArticle.getId(), newsArticle.getTitle(), list);
        articleWords.setLength(articleWords.getWords().size());
        List<ArticleWords> articleList  = new ArrayList<ArticleWords>(1);
        articleList.add(articleWords);
        return articleList.iterator();

    }
}
