package uk.ac.gla.dcs.bigdata.studentstructures;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

public class DPHResult implements Serializable {

    String id;

    String title;

    double score;

    public DPHResult() {
    }

    public DPHResult(String id) {
        this.id = id;
    }

    public DPHResult(String id, String title, double score) {
        this.id = id;
        this.title = title;
        this.score = score;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
