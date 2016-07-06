package tutorial;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import utils.Parse;
import utils.Tweet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
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
public class Ex3HashtagMining implements Serializable{

  private static String pathToFile = "data/reduced-tweets.json";
  
  public static void main(String[] args) {
	  Ex3HashtagMining ex3HashtagMining=new Ex3HashtagMining();
	  ex3HashtagMining.filterOnTweetTop10HashtagMining();
	
}

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
    JavaRDD<String> mentions = tweets.flatMap(new FlatMapFunction<Tweet, String>() {
		@Override
		public Iterable<String> call(Tweet tweet) throws Exception {
			return Arrays.asList(tweet.getText().split(" "));
		}
	}).filter(new Function<String, Boolean>() {

		@Override
		public Boolean call(String string) throws Exception {
			return string.startsWith("#")&&string.length()>1;
		}
	});

    return mentions;
  }

  /**
   *  Count how many times each hashtag is mentioned
   */
  public JavaPairRDD<String,Integer> countMentions() {
    JavaRDD<String> mentions = hashtagMentionedOnTweet();

    // Hint: think about what you did in the wordcount example
    // TODO write code here
    JavaPairRDD<String, Integer> counts = mentions.mapToPair(new PairFunction<String, String, Integer>() {

		@Override
		public Tuple2<String, Integer> call(String s) throws Exception {
			return new Tuple2<String, Integer>(s, 1);
		}
	}).reduceByKey(new Function2<Integer, Integer, Integer>() {
		
		@Override
		public Integer call(Integer a, Integer b) throws Exception {
			return a+b;
		}
	});

    return counts;
  }

  /**
   *  Find the 10 most popular Hashtags by descending order
   */
  public List<Tuple2<Integer, String>> top10HashTags() {
    JavaPairRDD<String, Integer> counts = countMentions();

    // Hint: take a look at the sorting and take methods
    // TODO write code here
    List<Tuple2<Integer, String>> top10 = (List<Tuple2<Integer, String>>) counts.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

		@Override
		public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2)
				throws Exception {
			return new Tuple2<Integer, String>(tuple2._2(), tuple2._1());
		}
    	
	}).sortByKey(false).take(10);

    return top10;
  }
  
  
  public void filterOnTweetTop10HashtagMining() {
		List<Tuple2<Integer, String>> output = top10HashTags();
		Iterator<Tuple2<Integer, String>> it2 = output.iterator();
		// List<Long> list=new ArrayList<Long>();
		while (it2.hasNext()) {
			Tuple2<Integer, String> tuple2 = (Tuple2<Integer, String>) it2
					.next();
			System.out.println(tuple2._1() + "==" + tuple2._2());

		}
		// System.out.println(tuple2._1()+"=="+ShowData(list));
		// list.clear();
	}

}