package uk.ac.gla.dcs.bigdata.studentstructures;

import it.unimi.dsi.fastutil.Hash;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ArticleWordsDic implements Serializable {

    private static final long serialVersionUID = 2156752235402082267L;

    String id; // unique article identifier

    String title; // article title

    int length;

    Map<String, Integer> map;

    public ArticleWordsDic() {
    }

    public ArticleWordsDic(String id, String title, int length, Map<String, Integer> map) {
        this.id = id;
        this.title = title;
        this.length = length;
        this.map = map;
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

    public Map<String, Integer> getMap() {
        return map;
    }

    public void setMap(Map<String, Integer> map) {
        this.map = map;
    }
}
