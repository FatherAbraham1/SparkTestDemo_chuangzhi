package tutorial;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import utils.Parse;
import utils.Tweet;

/**
 * The Java Spark API documentation:
 * http://spark.apache.org/docs/latest/api/java/index.html
 *
 * 我们使用包含了8198个tweet数据记录。数据格式如下：
 *
 * {"id":"572692378957430785", "user":"Srkian_nishu :)", "text":
 * "@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking"
 * , "place":"Orissa", "country":"India"}
 *
 * 目标：   找出user所有的tweet账户（一个user可能包含多个tweet账户，如Srkian_nishu的tweet账户有[572692378957430785，...]）
 *
 */
public class Ex1UserMining {

	private static String pathToFile = "data/reduced-tweets.json";

	public JavaRDD<Tweet> loadData() {
		// Create spark configuration and spark context
		SparkConf conf = new SparkConf().setAppName("User mining").set("spark.driver.allowMultipleContexts", "true")
				.setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the data and parse it into a Tweet.
		// Look at the Tweet Object in the TweetUtils class.
		JavaRDD<Tweet> tweets = sc.textFile(pathToFile).map(new Function<String, Tweet>() {
			public Tweet call(String line) throws Exception {
				// TODO Auto-generated method stub
				return Parse.parseJsonToTweet(line);
			}

		});

		return tweets;
	}

	/**
	 * For each user return all his tweets
	 */
	public JavaPairRDD<String, Iterable<Tweet>> tweetsByUser() {
		JavaRDD<Tweet> tweets = loadData();

		// TODO write code here
		// Hint: the Spark API provides a groupBy method
		JavaPairRDD<String, Iterable<Tweet>> tweetsByUser = null;

		return tweetsByUser;
	}

	/**
	 * Compute the number of tweets by user
	 */
	public JavaPairRDD<String, Integer> tweetByUserNumber() {
		JavaRDD<Tweet> tweets = loadData();

		// TODO write code here
		// Hint: think about what you did in the wordcount example
		JavaPairRDD<String, Integer> count = null;

		return count;
	}

}