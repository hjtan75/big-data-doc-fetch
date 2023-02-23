package uk.ac.gla.dcs.bigdata.studentfunctions.flatmap;

import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWords;

import java.util.Iterator;

public class GetWordsFlatMap implements FlatMapFunction<ArticleWords, String> {
    @Override
    public Iterator<String> call(ArticleWords articleWords) throws Exception {
        return articleWords.getWords().iterator();
    }
}
