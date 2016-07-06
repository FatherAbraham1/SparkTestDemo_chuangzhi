package tutorial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.apache.spark.util.TaskCompletionListener;

import scala.Function0;
import scala.Function1;
import scala.Tuple2;
import scala.runtime.BoxedUnit;
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
 * 目标： 找出user所有的tweet账户（一个user可能包含多个tweet账户，如Srkian_nishu的tweet账户有[
 * 572692378957430785，...]）
 *
 */
public class Ex1UserMining implements Serializable{

	private static String pathToFile = "data/reduced-tweets.json";
	
	public static void main(String[] args) {
		Ex1UserMining userMining=new Ex1UserMining();
		//userMining.tweetsByUser();
		userMining.filterOnTweetUser();
		System.exit(0);
	}

	public JavaRDD<Tweet> loadData() {
		// Create spark configuration and spark context
		SparkConf conf = new SparkConf().setAppName("User mining")
				.set("spark.driver.allowMultipleContexts", "true")
				.setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the data and parse it into a Tweet.
		// Look at the Tweet Object in the TweetUtils class.
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
	 * For each user return all his tweets
	 */
	public JavaPairRDD<String, Iterable<Tweet>> tweetsByUser() {
		JavaRDD<Tweet> tweets = loadData();

		// TODO write code here
		// Hint: the Spark API provides a groupBy method
		JavaPairRDD<String, Iterable<Tweet>> tweetsByUser = tweets.groupBy(new Function<Tweet, String>() {
			@Override
			public String call(Tweet tweet) throws Exception {
				
				return tweet.getUser();
			}
		});
		

		return tweetsByUser;
	}

	/**
	 * Compute the number of tweets by user
	 */
	public JavaPairRDD<String, Integer> tweetByUserNumber() {
		JavaRDD<Tweet> tweets = loadData();

		// TODO write code here
		// Hint: think about what you did in the wordcount example
		JavaPairRDD<String, Integer> count = tweets.mapToPair(new PairFunction<Tweet, String	,Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tweet tweet) throws Exception {
				return  new Tuple2<String, Integer>(tweet.getUser(), 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a+b;
			}
		});

		return count;
	}
	
	public void filterOnTweetUser() {
		JavaPairRDD<String, Iterable<Tweet>> filtered = tweetsByUser();
		
		//filtered.values().splits().get(0);
	 filtered.keyBy(new Function<Tuple2<String,Iterable<Tweet>>,Tuple2<String,ArrayList<Long>> >() {

		@Override
		public Tuple2<String, ArrayList<Long>> call(
				Tuple2<String, Iterable<Tweet>> t) throws Exception {
			ArrayList< Long> arrayList=new ArrayList<Long>();
			arrayList.add(t._2().iterator().next().getId());
			return new Tuple2<String, ArrayList<Long>>(t._1(),arrayList);
		}
	});
	 
		List<Tuple2<String, Iterable<Tweet>>> output = filtered.collect();
		Iterator< Tuple2<String, Iterable<Tweet>>> it2=output.iterator();
		List<Long> list=new ArrayList<Long>();
		while (it2.hasNext()) {
			Tuple2<String, Iterable<utils.Tweet>> tuple2 = (Tuple2<String, Iterable<utils.Tweet>>) it2.next();
			Iterator<Tweet> iterator= tuple2._2().iterator();
			while (iterator.hasNext()) {
				Tweet tweet = (Tweet) iterator.next();
				list.add(tweet.getId());
			//	System.out.println(tuple2._1()+"=="+tweet.getId());
				
			}
			System.out.println(tuple2._1()+"=="+ShowData(list));
			list.clear();
		}
		//return filtered;
		
	}
	
	public String ShowData(List<Long> list) {
		String str="[";
		for (int i = 0; i < list.size(); i++) {
			str+=list.get(i)+",";
		}
		return str+"]";
	}
	
	public JavaPairRDD<String, Integer> filterOnTweetcount() {
		JavaPairRDD<String, Integer> tweetcounts = tweetByUserNumber();
		
		List<Tuple2<String, Integer>> output = tweetcounts.collect();
		JavaPairRDD<String, Integer> filtered = null;
		// int count=0;
		for (Tuple2<?, ?> tuple : output) {
				// filtered=tuple;
				System.out.println(tuple._1() + "==" + tuple._2());

		}
		return filtered;
	}

}