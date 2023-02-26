# Project

# 1. Preprocessing

## Process: NewsArticle → ArticleWordsDic

1. Get the unique query term, store them in a HashSet — method `getQueryWordsSet()`.
2. Create a custom HashMap Accumulator — `TotalQueryWordsAccumulator`, and 2 Long Accumulator:
    - `wordCountAccumulator`: The overall all document length after preprocessing.
    - `docCountAccumulator`: The total doc number after preprocessing.
    - `TotalQueryWordsAccumulator`: Count the total number of **Query** term appears in the whole corpus.

   And using above variables, we can get `averageDocumentLengthInCorpus`.

3. Use `PreprocessFlatMap()` to convert `NewsArticle` DataSet to `ArticleWordsDic` DataSet and collect, in order to lower the data size, only convert the article has query term to ArticleWordsDic.
    1. Check if the title or ID is null, and the get 5 paragraph and title into a List<String>.
    2. `wordCountAccumulator` += list size, and `docCountAccumulator` + 1.
    3. Use the query term hashset, count each term and frequency in current Article, store in a HashMap.
    4. If the map size == 0, means that no term showed up in this Article, return an empty list.
    5. `TotalQueryWordsAccumulator` add the HashMap.
    6. Return the created `ArticleWordsDic`
4. Retrieve above DPH parameter and the HashMap.

## Structures & Functions

### Class `ArticleWordsDic`

```java
public class ArticleWordsDic implements Serializable {

    String id; // unique article identifier

    String title; // article title

    int length;

		Map<String, Integer> map;
}
```

### Class `TotalQueryWordsAccumulator`

```java
public class TotalQueryWordsAccumulator extends AccumulatorV2<HashMap<String, Integer>, HashMap<String, Integer>> {

    private HashMap<String, Integer> hashMap = new HashMap<>();
		....
}
```

### FlatMapFunction `PreprocessFlatMap`

### FlatMapFunction `QueryWordsFlatMap`

# 2. Query

## Process: ArticleWordsDic → QueryResultWithArticleId

1. Wrap the DPH parameter and the List<ArticleWordsDic> into Broadcast variables.
2. Based on the queries dataset, call `QueryToQueryResultMap` method to get the `QueryResultWithArticleId`.
    1. Get the score of each article, store the articleID and score into `List<DPHResult> dphResultList`
    2. Sort the `dphResultList`.
    3. Calculate the distance between titles, and create 10 article list, storing at `queryResultWithArticleId`.
    4. Return the `queryResultWithArticleId`.

## Structures & Functions

### Class `QueryResultWithArticleId`

```java
public class QueryResultWithArticleId implements Serializable {

    Query query;

    List<DPHResult> articleIdList;
}
```

### Class `DPHResult`

```java
public class DPHResult implements Serializable {

    String id;

    String title;

    double score;
}
```

### MapFunction `QueryToQueryResultMap`

```java
public class QueryToQueryResultMap implements MapFunction<Query, QueryResultWithArticleId> {

    private static final long serialVersionUID = -484410270146328326L;

    Broadcast<List<ArticleWordsDic>> listBroadcast;
    Broadcast<Long> totalDocsInCorpus;
    Broadcast<Double> averageDocumentLengthInCorpus;
    Broadcast<Map<String, Integer>> totalTermFrequencyInCorpusDic;

    public QueryToQueryResultMap(Broadcast<List<ArticleWordsDic>> listBroadcast, Broadcast<Long> totalDocsInCorpus,
                                 Broadcast<Double> averageDocumentLengthInCorpus,
                                 Broadcast<Map<String, Integer>> totalTermFrequencyInCorpusDic) {
        this.listBroadcast = listBroadcast;
        this.totalDocsInCorpus = totalDocsInCorpus;
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
        this.totalTermFrequencyInCorpusDic = totalTermFrequencyInCorpusDic;
    }
}
```

# 3. Generate DocumentRanking

## Process: QueryResultWithArticleId → DocumentRanking

1. In `Dataset<QueryResultWithArticleId> queryResultWithArticleIdDataset`, there are all the ArticleID needed for the result, then using `GetReusltArticleIdFlatMap()` to generate the unique ArticleID as HashSet, use it on `NewsArticleResultFlatMap` of news dataset to get the Dataset<NewsArticle> needed for create DocumentRanking.
2. After getting Dataset<NewsArticle>, map it to `JavaPairRDD<String, NewsArticle>`, the key is the ArticleId, collect as Map.
3. Then broadcast it to the previous `queryResultWithArticleIdDataset`, using `QueryWithArticleIdToDR` Map to get the final `List<DocumentRanking> documentRankingList`.

## Functions

### FlatMapFunction `GetReusltArticleIdFlatMap`

### FlatMapFunction `NewsArticleResultFlatMap`

### MapFunction `QueryWithArticleIdToDR`