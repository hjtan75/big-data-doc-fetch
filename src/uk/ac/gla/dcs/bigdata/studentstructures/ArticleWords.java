package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

public class ArticleWords implements Serializable {


    private static final long serialVersionUID = 2156752235108082267L;
    String id; // unique article identifier
    String title; // article title
    List<String> words;

    public ArticleWords(){}

    public ArticleWords(String id, String title, List<String> words) {
        this.id = id;
        this.title = title;
        this.words = words;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getWords() {
        return words;
    }

    public void setWords(List<String> words) {
        this.words = words;
    }


}
