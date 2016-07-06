package tutorial;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import utils.Parse;
import utils.Tweet;

import java.util.Map;

/**
 
 * 目标 ： 建立标记的索引视图
 * 
 * 说明：  例如对于标记#spark，它出现在tweet1, tweet3, tweet39中。 建立的索引应该返回(#spark, List(tweet1,tweet3, tweet39))
 * 
 */
public class Ex4InvertedIndex {

  private static String pathToFile = "data/reduced-tweets.json";

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  public JavaRDD<Tweet> loadData() {
    // create spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("Inverted index")
        .set("spark.driver.allowMultipleContexts", "true")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

	JavaRDD<Tweet> tweets = sc.textFile(pathToFile).map(new Function<String, Tweet>() {
		public Tweet call(String line) throws Exception {
			// TODO Auto-generated method stub
			return Parse.parseJsonToTweet(line);
		}

	});

    return tweets;
  }

  public Map<String, Iterable<Tweet>> invertedIndex() {
    JavaRDD<Tweet> tweets = loadData();

    // for each tweet, extract all the hashtag and then create couples (hashtag,tweet)
    // Hint: see the flatMapToPair method
    // TODO write code here
    JavaPairRDD<String, Tweet> pairs = null;

    // We want to group the tweets by hashtag
    // TODO write code here
    JavaPairRDD<String, Iterable<Tweet>> tweetsByHashtag = null;

    // Then return the inverted index (= a map structure)
    // TODO write code here
    Map<String, Iterable<Tweet>> map = null;

    return map;
  }

}