package tutorial;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import utils.Parse;
import utils.Tweet;

import java.util.List;

/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 * 我们使用包含了8198个tweet数据记录。数据格式如下：
 *
 * {"id":"572692378957430785", "user":"Srkian_nishu :)", "text":
 * "@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking"
 * , "place":"Orissa", "country":"India"}
 * 
 * 目标： 1.找出所有所有被标记（”#“）到的人。
 * 	    2.找出每个被标记（“#”）的人被（”@“）到的次数，求出次数前十
 * 
 *
 */
public class Ex3HashtagMining {

  private static String pathToFile = "data/reduced-tweets.json";

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  public JavaRDD<Tweet> loadData() {
    // create spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("Hashtag mining")
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

  /**
   *  Find all the hashtags mentioned on tweets
   */
  public JavaRDD<String> hashtagMentionedOnTweet() {
    JavaRDD<Tweet> tweets = loadData();

    // You want to return an RDD with the mentions
    // Hint: think about separating the word in the text field and then find the mentions
    // TODO write code here
    JavaRDD<String> mentions = null;

    return mentions;
  }

  /**
   *  Count how many times each hashtag is mentioned
   */
  public JavaPairRDD<String,Integer> countMentions() {
    JavaRDD<String> mentions = hashtagMentionedOnTweet();

    // Hint: think about what you did in the wordcount example
    // TODO write code here
    JavaPairRDD<String, Integer> counts = null;

    return counts;
  }

  /**
   *  Find the 10 most popular Hashtags by descending order
   */
  public List<Tuple2<Integer, String>> top10HashTags() {
    JavaPairRDD<String, Integer> counts = countMentions();

    // Hint: take a look at the sorting and take methods
    // TODO write code here
    List<Tuple2<Integer, String>> top10 = null;

    return top10;
  }

}