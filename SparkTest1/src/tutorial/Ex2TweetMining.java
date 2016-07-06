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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * The Java Spark API documentation:
 * http://spark.apache.org/docs/latest/api/java/index.html
 * 我们使用包含了8198个tweet数据记录。数据格式如下：
 *
 * {"id":"572692378957430785", "user":"Srkian_nishu :)", "text":
 * "@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking"
 * , "place":"Orissa", "country":"India"}
 * 
 * 目标： 1.找出所有被@的人 2.计算每个人被@到的次数，找出前10个@次数最多的人
 * 
 *
 * Use the Ex2TweetMiningTest to implement the code.
 */
public class Ex2TweetMining implements Serializable {

	/**
	 * 
	 */

	private static String pathToFile = "data/reduced-tweets.json";
	private static String saveAsTextFile = "out1/out1.txt";

	/**
	 * Load the data from the json file and return an RDD of Tweet
	 */
	public JavaRDD<Tweet> loadData() {
		// create spark configuration and spark context
		SparkConf conf = new SparkConf().setAppName("Tweet mining").setMaster(
				"local[*]");
		// .setMaster("spark://master:7077");
		conf.set("spark.driver.allowMultipleContexts", "true");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// sc.addJar("/home/sun/jars/tutorial-all.jar");

		// load the data and create an RDD of Tweet
		// JavaRDD<Tweet> tweets =
		// sc.textFile("hdfs://master:9000/sparkdata/reduced-tweets.json")
		JavaRDD<Tweet> tweets = sc.textFile(pathToFile).map(
				new Function<String, Tweet>() {
					public Tweet call(String line) throws Exception {
						// TODO Auto-generated method stub
						return Parse.parseJsonToTweet(line);
					}

				});
		return tweets;
	}

	/**
	 * Find all the persons mentioned on tweets (case sensitive)
	 */
	public JavaRDD<String> mentionOnTweet() {
		JavaRDD<Tweet> tweets = loadData();

		// You want to return an RDD with the mentions
		// Hint: think about separating the word in the text field and then find
		// the mentions
		// TODO write code here
		JavaRDD<String> mentions = tweets
				.flatMap(new FlatMapFunction<Tweet, String>() {
					public Iterable<String> call(Tweet t) throws Exception {
						String text = t.getText();
						Set<String> set = new HashSet<String>();
						String[] words = text.split(" ");
						for (String word : words) {
							if (word.startsWith("@")) {
								set.add(word);
							}
						}
						return set;
					}

				});

		return mentions;

	}

	/**
	 * Count how many times each person is mentioned
	 */
	public JavaPairRDD<String, Integer> countMentions() {
		JavaRDD<String> mentions = mentionOnTweet();

		// Hint: think about what you did in the wordcount example
		// TODO write code here
		JavaPairRDD<String, Integer> mentionCount = mentions.mapToPair(
				new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String t)
							throws Exception {
						return new Tuple2<String, Integer>(t, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});
		// mentionCount.saveAsTextFile("hdfs://master:9000/sparkdata/tweets-m4");
		// mentionCount.saveAsTextFile(saveAsTextFile);
		return mentionCount;
	}

	/**
	 * Find the 10 most mentioned persons by descending order
	 */
	public List<Tuple2<Integer, String>> top10mentions() {
		JavaPairRDD<String, Integer> counts = countMentions();

		// Hint: take a look at the sorting and take methods
		// TODO write code here
		List<Tuple2<Integer, String>> mostMentioned = (List<Tuple2<Integer, String>>) counts
				.mapToPair(
						new PairFunction<Tuple2<String, Integer>, Integer, String>() {

							@Override
							public Tuple2<Integer, String> call(
									Tuple2<String, Integer> tuple2)
									throws Exception {

								return new Tuple2<Integer, String>(tuple2._2(),
										tuple2._1());
							}
						}).sortByKey(false).take(10);

		return mostMentioned;
	}

	public void filterOnTweetTop10Mentions() {
		List<Tuple2<Integer, String>> output = top10mentions();
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

	// return filtered;

	public static void main(String[] args) {
		Ex2TweetMining ex2TweetMining = new Ex2TweetMining();
		ex2TweetMining.filterOnTweetTop10Mentions();

		/*
		 * JavaPairRDD<String, Integer> res = ex2TweetMining.countMentions();
		 * System.out.println(res.take(1));
		 */
	}
}