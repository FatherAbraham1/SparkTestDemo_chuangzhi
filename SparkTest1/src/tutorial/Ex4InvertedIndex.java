package tutorial;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import utils.Parse;
import utils.Tweet;

/**
 
 * 目标 ： 建立标记的索引视图
 * 
 * 说明：  例如对于标记#spark，它出现在tweet1, tweet3, tweet39中。 建立的索引应该返回(#spark, List(tweet1,tweet3, tweet39))
 * 
 */
public class Ex4InvertedIndex implements Serializable{

  private static String pathToFile = "data/reduced-tweets.json";
  
  public static void main(String[] args) {
	  ShowData();
}

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  public static JavaRDD<Tweet> loadData() {
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

  
  public  static void ShowData(){
	  Map<String, Iterable<Tweet>> output=invertedIndex();
	  
	  Iterator<Entry<String, Iterable<Tweet>>> it2 = output.entrySet().iterator();
	  
		List<Long> list=new ArrayList<Long>();
		while (it2.hasNext()) {
			Entry<String, Iterable<Tweet>> tuple2 =  it2.next();
			Iterator<Tweet> iterator= tuple2.getValue().iterator();
			while (iterator.hasNext()) {
				Tweet tweet = (Tweet) iterator.next();
				list.add(tweet.getId());
			//	System.out.println(tuple2._1()+"=="+tweet.getId());
				
			}
			System.out.println(tuple2.getKey()+ "==" +PrintData(list));
			list.clear();

		}
	  
	  
  }
  
  public static String PrintData(List<Long> list ){
		String str="[";
		for (int i = 0; i < list.size(); i++) {
			str+=list.get(i)+",";
		}
		return str+"]";
  }
  
  public  static void ShowData1(){
	  Map<String, Iterable<Tweet>> output=invertedIndex();
	  
	  Iterator<Entry<String, Iterable<Tweet>>> it2 = output.entrySet().iterator();
	  
		// List<Long> list=new ArrayList<Long>();
		while (it2.hasNext()) {
			Entry<String, Iterable<Tweet>> tuple2 =  it2.next();
			
			System.out.println(tuple2.getKey()+ "==" + tuple2.getValue());

		}
	  
	  
  }
  
  
  public static Map<String, Iterable<Tweet>> invertedIndex() {
    JavaRDD<Tweet> tweets = loadData();

    // for each tweet, extract all the hashtag and then create couples (hashtag,tweet)
    // Hint: see the flatMapToPair method
    // TODO write code here
    JavaPairRDD<String, Tweet> pairs = tweets.flatMapToPair(new PairFlatMapFunction<Tweet, String, Tweet>() {

		@Override
		public Iterable<Tuple2<String, Tweet>> call(Tweet tweet)
				throws Exception {
			List results = new ArrayList();
		      List<String> hashtags = new ArrayList();
		      List<String> words = Arrays.asList(tweet.getText().split(" "));

		      for (String word: words) {
		        if (word.startsWith("#") && word.length() > 1) {
		          hashtags.add(word);
		        }
		      }

		      for (String hashtag : hashtags) {
		        Tuple2<String, Tweet> result = new Tuple2<>(hashtag, tweet);
		        results.add(result);
		      }
			return results;
		}
	});

    // We want to group the tweets by hashtag
    // TODO write code here
    JavaPairRDD<String, Iterable<Tweet>> tweetsByHashtag = pairs.groupByKey();

    // Then return the inverted index (= a map structure)
    // TODO write code here
    Map<String, Iterable<Tweet>> map =tweetsByHashtag.collectAsMap();

    return map;
  }

}