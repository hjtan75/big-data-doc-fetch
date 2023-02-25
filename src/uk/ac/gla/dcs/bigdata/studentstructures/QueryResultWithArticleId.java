package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.io.Serializable;
import java.util.List;

public class QueryResultWithArticleId implements Serializable {

    Query query;

    List<DPHResult> articleIdList;

    public QueryResultWithArticleId() {
    }

    public QueryResultWithArticleId(Query query, List<DPHResult> articleIdList) {
        this.query = query;
        this.articleIdList = articleIdList;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public List<DPHResult> getArticleIdList() {
        return articleIdList;
    }

    public void setArticleIdList(List<DPHResult> articleIdList) {
        this.articleIdList = articleIdList;
    }
}
