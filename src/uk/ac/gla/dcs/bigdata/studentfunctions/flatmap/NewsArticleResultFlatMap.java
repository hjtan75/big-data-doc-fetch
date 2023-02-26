package uk.ac.gla.dcs.bigdata.studentfunctions.flatmap;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.collection.View;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class NewsArticleResultFlatMap implements FlatMapFunction<NewsArticle, NewsArticle> {

    Broadcast<HashSet<String>> hashSetBroadcast;

    public NewsArticleResultFlatMap() {
    }

    public NewsArticleResultFlatMap(Broadcast<HashSet<String>> hashSetBroadcast) {
        this.hashSetBroadcast = hashSetBroadcast;
    }

    @Override
    public Iterator<NewsArticle> call(NewsArticle newsArticle) throws Exception {
        var hashSet = hashSetBroadcast.value();
        if (hashSet.contains(newsArticle.getId())){
            List<NewsArticle> newsArticleList  = new ArrayList<NewsArticle>(1);
            newsArticleList.add(newsArticle);
            return newsArticleList.iterator();
        }
        List<NewsArticle> newsArticleList  = new ArrayList<NewsArticle>(0);
        return newsArticleList.iterator();
    }
}
