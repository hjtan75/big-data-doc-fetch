package uk.ac.gla.dcs.bigdata.studentfunctions.map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWords;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWordsDic;

import java.util.HashMap;
import java.util.Set;

public class WordsToDicMap implements MapFunction<ArticleWords, ArticleWordsDic> {

    private static final long serialVersionUID = -484814270146328326L;

    Broadcast<Set<String>> broadcastQueryWords;

    public WordsToDicMap(Broadcast<Set<String>> broadcastQueryWords) {
        this.broadcastQueryWords = broadcastQueryWords;
    }

    @Override
    public ArticleWordsDic call(ArticleWords articleWords) throws Exception {
        ArticleWordsDic articleWordsDic = new ArticleWordsDic(articleWords.getId(), articleWords.getTitle(), articleWords.getLength());
        Set<String> queryWords = broadcastQueryWords.value();
        HashMap<String, Integer> mapping = articleWordsDic.getMapping();
        for (String s : articleWords.getWords()){
            if (queryWords.contains(s)){
                if (mapping.containsKey(s)){
                    mapping.put(s, mapping.get(s) + 1);
                }else{
                    mapping.put(s, 1);
                }
            }
        }
        return articleWordsDic;
    }
}
