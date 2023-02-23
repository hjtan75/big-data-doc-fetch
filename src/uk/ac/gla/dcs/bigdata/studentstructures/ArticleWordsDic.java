package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.HashMap;

public class ArticleWordsDic {

    private static final long serialVersionUID = 2156752235402082267L;

    String id; // unique article identifier

    String title; // article title

    int length;

    HashMap<String, Integer> mapping;

    public ArticleWordsDic() {
    }

    public ArticleWordsDic(String id, String title, int length) {
        this.id = id;
        this.title = title;
        this.length = length;
        this.mapping = new HashMap<>();
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

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public HashMap<String, Integer> getMapping() {
        return mapping;
    }

    public void setMapping(HashMap<String, Integer> mapping) {
        this.mapping = mapping;
    }
}
