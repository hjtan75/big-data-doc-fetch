package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWords;

import java.util.ArrayList;

public class PreprocessMap implements MapFunction<NewsArticle, ArticleWords> {

    private static final long serialVersionUID = 1938637539215783780L;
    private transient TextPreProcessor processor;

    @Override
    public ArticleWords call(NewsArticle newsArticle) throws Exception {
        if (newsArticle.getTitle() == null || newsArticle.getId() == null){
            return null;
        }
        if (processor==null) processor = new TextPreProcessor();
        var list = new ArrayList<String>();
        if (newsArticle.getTitle() != null){
            list.addAll(processor.process(newsArticle.getTitle()));
        }
        var number = 0;
        var iterator = newsArticle.getContents().iterator();
        while(iterator.hasNext() && number < 5){
            var content = iterator.next();
            if (content.getSubtype()!= null && content.getSubtype().equals("paragraph")){
                if (content.getContent() != null)
                list.addAll(processor.process(content.getContent()));
                number++;
            }
        }
        return new ArticleWords(newsArticle.getId(), newsArticle.getTitle(), list);
    }
}
