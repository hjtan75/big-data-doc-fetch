package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;

import scala.Tuple2;
import scala.reflect.internal.Trees;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.filter.FilterArticleWithQueryFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.flatmap.GetWordsFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.filter.PreprocessFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.flatmap.QueryWordsFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.QueryToDRMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.map.WordsToDicMap;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWords;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleWordsDic;

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
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
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
		//1. remove stopwords (words with little discriminative value, e.g. ‘the’) and apply stemming
		// (which converts each word into its ‘stem’, a shorter version that helps with term mismatch between documents and queries).
		System.out.println(news.count());
		Encoder<ArticleWords> newsArticleEncoder = Encoders.bean(ArticleWords.class);
		Dataset<ArticleWords> articleWordsDataSet = news.flatMap(new PreprocessFlatMap(), newsArticleEncoder);
		System.out.println(articleWordsDataSet.count());


		// 2. calculate DPH parameter
		// 2.1 get the query word map
		Set<String> queryWordSet = getQueryWordsSet(queries);

		// 2.2 get totalTermFrequencyInCorpus map, totalDocsInCorpus, averageDocumentLengthInCorpus
		Dataset<String> Words = articleWordsDataSet.flatMap(new GetWordsFlatMap(), Encoders.STRING());
		System.out.println("Words Count " + Words.count());

		long totalCorpusLength = Words.count();
		long totalDocsInCorpus = articleWordsDataSet.count();
		double averageDocumentLengthInCorpus = totalCorpusLength / totalDocsInCorpus;
		JavaPairRDD<String,Integer> articlePairs=Words.toJavaRDD().mapToPair(new PairFunction<String,String,Integer>(){
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String,Integer>(word,1);
			}
		});
		JavaPairRDD<String,Integer> articleWordCount = articlePairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		Map<String, Integer> totalTermFrequencyInCorpus = articleWordCount.collectAsMap();


		// 2.3 create word map for each article
		Broadcast<Set<String>> broadcastQueryWords = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryWordSet);
		// add the <word, time> map into articleWordsDicDataSet
		Encoder<ArticleWordsDic> articleWordsDicEncoder = Encoders.bean(ArticleWordsDic.class);
		Dataset<ArticleWordsDic> articleWordsDicDataSet = articleWordsDataSet.map(new WordsToDicMap(broadcastQueryWords), articleWordsDicEncoder);
		System.out.println("Article Words Dic " + articleWordsDicDataSet.count());
		// filter the article doesn't have any query term
		Dataset<ArticleWordsDic> articleWordsDicAfterFilter = articleWordsDicDataSet.flatMap(new FilterArticleWithQueryFlatMap(), articleWordsDicEncoder);
//		Dataset<ArticleWordsDic> articleWordsDicAfterFilter = articleWordsDicDataSet.filter(new FilterFunction<ArticleWordsDic>() {
//			@Override
//			public boolean call(ArticleWordsDic articleWordsDic) throws Exception {
//				var mapping = articleWordsDic.getMap();
//				if (mapping == null || mapping.size() == 0){
//					List<ArticleWordsDic> articleWordsDicList  = new ArrayList<>(0);
//					return false;
//				}
//				return true;
//			}
//		});
		List<ArticleWordsDic> articleWordsDicList = articleWordsDicAfterFilter.collectAsList();

		for (var a : articleWordsDicList){
			if (a.getMap() == null){
				System.out.println("In main thread mapping null " + a.getId());
			}else{
				System.out.println("In main thread mapping " + a.getId() + " " + a.getMap().size());
			}
		}


		System.out.println("Article Words Dic after Filter " + articleWordsDicAfterFilter.count());


//		try {
//			Thread.sleep(1000000);
//		} catch (InterruptedException e) {
//			throw new RuntimeException(e);
//		}
		// 3 calculate the query
		JavaPairRDD<String, NewsArticle> newsArticleMapRDD = news.toJavaRDD().mapToPair(new PairFunction<NewsArticle,String,NewsArticle>(){
			@Override
			public Tuple2<String, NewsArticle> call(NewsArticle newsArticle) throws Exception {
				return new Tuple2<String, NewsArticle>(newsArticle.getId(), newsArticle);
			}
		});

		Broadcast<Map<String, NewsArticle>> newsArticleMapBroadcast =  JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(newsArticleMapRDD.collectAsMap());

		System.out.println("totalDocsInCorpus: " + totalDocsInCorpus);
		System.out.println("averageDocumentLengthInCorpus: " + averageDocumentLengthInCorpus);
		Broadcast<Long> totalDocsInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalDocsInCorpus);
		Broadcast<Double> averageDocumentLengthInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLengthInCorpus);
		Broadcast<Map<String, Integer>> totalTermFrequencyInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(totalTermFrequencyInCorpus);
		Broadcast<List<ArticleWordsDic>> articleWordsDicListBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(articleWordsDicList);

		Dataset<DocumentRanking> documentRankingDataset = queries.map(new QueryToDRMap(articleWordsDicListBroadcast,
				totalDocsInCorpusBroadcast, averageDocumentLengthInCorpusBroadcast,
				totalTermFrequencyInCorpusBroadcast, newsArticleMapBroadcast), Encoders.bean(DocumentRanking.class));

		List<DocumentRanking> documentRankingList = documentRankingDataset.collectAsList();
		return documentRankingList; // replace this with the the list of DocumentRanking output by your topology
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


