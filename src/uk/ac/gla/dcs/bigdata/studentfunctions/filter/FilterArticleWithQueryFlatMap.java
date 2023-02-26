package uk.ac.gla.dcs.bigdata.studentfunctions.filter;

import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWordsDic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class FilterArticleWithQueryFlatMap implements FlatMapFunction<ArticleWordsDic, ArticleWordsDic> {
    @Override
    public Iterator<ArticleWordsDic> call(ArticleWordsDic articleWordsDic) throws Exception {
        var mapping = articleWordsDic.getMap();
        if (mapping == null || mapping.size() == 0){
            List<ArticleWordsDic> articleWordsDicList  = new ArrayList<>(0);
            return articleWordsDicList.iterator();
        }
        List<ArticleWordsDic> articleWordsDicList  = new ArrayList<>(1);
        articleWordsDicList.add(new ArticleWordsDic(articleWordsDic.getId(), articleWordsDic.getTitle(), articleWordsDic.getLength(), new HashMap<>(articleWordsDic.getMap())));
        //System.out.println("In FilterArticleWithQueryFlatMap: " + articleWordsDicList.get(0).getId() + " " +articleWordsDicList.get(0).getMap().size());
        return articleWordsDicList.iterator();
    }
}
