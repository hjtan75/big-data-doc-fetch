package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;

import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentfunctions.TotalQueryWordsAccumulator;
import uk.ac.gla.dcs.bigdata.studentfunctions.flatmap.GetReusltArticleIdFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.filter.PreprocessFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.flatmap.NewsArticleResultFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.flatmap.QueryWordsFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.QueryToQueryResultMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.QueryWithArticleIdToDR;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWordsDic;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryResultWithArticleId;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		//if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; // default is a sample of 5000 news articles
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle


		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------

		// Step 1. Preprocessing
		// NewsArticle → ArticleWordsDic
        // 1.1 Get the query word map and create broadcast
        Set<String> queryWordSet = getQueryWordsSet(queries);
        Broadcast<Set<String>> broadcastQueryWords = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryWordSet);
        // 1.2 Change news -> ArticleWordsDic, remove stopwords (words with little discriminative value, e.g. ‘the’) and apply stemming
		LongAccumulator wordCountAccumulator = spark.sparkContext().longAccumulator();
		LongAccumulator docCountAccumulator = spark.sparkContext().longAccumulator();
		TotalQueryWordsAccumulator totalQueryWordsAccumulator = new TotalQueryWordsAccumulator();
		spark.sparkContext().register(totalQueryWordsAccumulator);

		Dataset<ArticleWordsDic> articleWordsDicDataset = news.flatMap(new PreprocessFlatMap(wordCountAccumulator, docCountAccumulator, totalQueryWordsAccumulator, broadcastQueryWords), Encoders.bean(ArticleWordsDic.class));

		// get totalTermFrequencyInCorpus map, totalDocsInCorpus, averageDocumentLengthInCorpus
		List<ArticleWordsDic> articleWordsDicList = articleWordsDicDataset.collectAsList();
		long totalCorpusLength = wordCountAccumulator.value();
		long totalDocsInCorpus = docCountAccumulator.value();
		double averageDocumentLengthInCorpus = totalCorpusLength / totalDocsInCorpus;
		Map<String, Integer> totalTermFrequencyInCorpus = totalQueryWordsAccumulator.value();

		System.out.println("totalDocsInCorpus: " + totalDocsInCorpus);
		System.out.println("totalCorpusLength: " + totalCorpusLength);
		System.out.println("averageDocumentLengthInCorpus: " + averageDocumentLengthInCorpus);

		// Step 2. calculate the query
		// ArticleWordsDic → QueryResultWithArticleId
		//  1. Wrap the DPH parameter and the List<ArticleWordsDic> into Broadcast variables.
		//  2. Based on the queries dataset, call `QueryToQueryResultMap` method to get the `QueryResultWithArticleId`.
		//    1. Get the score of each article, store the articleID and score into `List<DPHResult> dphResultList`
		//    2. Sort the `dphResultList`.
		//    3. Calculate the distance between titles, and create 10 article list, storing at `queryResultWithArticleId`.
		//    4. Return the `queryResultWithArticleId`.
		Broadcast<Long> totalDocsInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocsInCorpus);
		Broadcast<Double> averageDocumentLengthInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLengthInCorpus);
		Broadcast<Map<String, Integer>> totalTermFrequencyInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalTermFrequencyInCorpus);
		Broadcast<List<ArticleWordsDic>> articleWordsDicListBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(articleWordsDicList);


		Dataset<QueryResultWithArticleId> queryResultWithArticleIdDataset = queries.map(new QueryToQueryResultMap(articleWordsDicListBroadcast,
				totalDocsInCorpusBroadcast, averageDocumentLengthInCorpusBroadcast,
				totalTermFrequencyInCorpusBroadcast), Encoders.bean(QueryResultWithArticleId.class));


		// Step 3. Generate DocumentRanking
		// QueryResultWithArticleId → DocumentRanking
		//  1. In `Dataset<QueryResultWithArticleId> queryResultWithArticleIdDataset`, there are all the ArticleID needed for the result, then using `GetReusltArticleIdFlatMap()` to generate the unique ArticleID as HashSet,
		//     use it on `NewsArticleResultFlatMap` of news dataset to get the Dataset<NewsArticle> needed for create DocumentRanking.
		//  2. After getting Dataset<NewsArticle>, map it to `JavaPairRDD<String, NewsArticle>`, the key is the ArticleId, collect as Map.
		//  3. Then broadcast it to the previous `queryResultWithArticleIdDataset`, using `QueryWithArticleIdToDR` Map to get the final `List<DocumentRanking> documentRankingList`.
		Dataset<String> queryResultWithArticleIdList  = queryResultWithArticleIdDataset.flatMap(new GetReusltArticleIdFlatMap(), Encoders.STRING()).distinct();
		HashSet<String> resultArticleIdSet = new HashSet<>(queryResultWithArticleIdList.collectAsList());
		Broadcast<HashSet<String>> resultArticleIdSetBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(resultArticleIdSet);
		Dataset<NewsArticle> resultNews = news.flatMap(new NewsArticleResultFlatMap(resultArticleIdSetBroadcast), Encoders.bean(NewsArticle.class));

		JavaPairRDD<String, NewsArticle> newsArticleMapRDD = resultNews.toJavaRDD().mapToPair(new PairFunction<NewsArticle,String,NewsArticle>(){
			@Override
			public Tuple2<String, NewsArticle> call(NewsArticle newsArticle) throws Exception {
				return new Tuple2<String, NewsArticle>(newsArticle.getId(), newsArticle);
			}
		});
		Broadcast<Map<String, NewsArticle>> newsArticleMapBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(newsArticleMapRDD.collectAsMap());
		var documentRankingDataset = queryResultWithArticleIdDataset.map(new QueryWithArticleIdToDR(newsArticleMapBroadcast), Encoders.bean(DocumentRanking.class));

		List<DocumentRanking> documentRankingList = documentRankingDataset.collectAsList();
		for (DocumentRanking documentRanking : documentRankingList) {
			System.out.println("Query: " + documentRanking.getQuery().getOriginalQuery());
			for (RankedResult rankedResult : documentRanking.getResults()) {
				System.out.println("Document ID: " + rankedResult.getDocid());
			}
			System.out.println("-----------------------------------------------");
		}
		
		return documentRankingList;
	}

	private static Set<String> getQueryWordsSet(Dataset<Query> queries){
		Dataset<String> queryWords = queries.flatMap(new QueryWordsFlatMap(), Encoders.STRING());
		JavaPairRDD<String,Integer> queryPairs = queryWords.toJavaRDD().mapToPair(new PairFunction<String,String,Integer>(){
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String,Integer>(word,1);
			}
		});
		JavaPairRDD<String,Integer> queryWordCount = queryPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		Set<String> queryWordSet = new HashSet(queryWordCount.collectAsMap().keySet());
		return queryWordSet;
	}
}


