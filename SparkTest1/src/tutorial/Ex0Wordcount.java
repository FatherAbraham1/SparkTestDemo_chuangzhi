package tutorial;

import java.io.Serializable;

import java.util.Arrays;
import java.util.List;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


/*
 *  step 1, the mapper:
 *  
 *  -我们为每一个单词添加属性 1.获取形如（word,1）的 JavaPairRDD<String, Integer>。单词作为key
 *
 *  step 2, the reducer:
 *  -合并统计.
 *
 *  
 */
public class Ex0Wordcount implements Serializable {
	public static String pathToFile = "data/wordcount.txt";
	public static SparkConf conf = null;
	public static JavaSparkContext sc = null;

	static {

		conf = new SparkConf().setAppName("Wordcount")
				.set("spark.driver.allowMultipleContexts", "true");
				//.setMaster("spark://master:7077");
		conf.set("spark.executor.memory", "1000m");
		conf .setMaster("local[*]"); // here local mode. And * means you will use
		// as much as you have cores.

		sc = new JavaSparkContext(conf);
		sc.addJar("/home/hadoop/tools/jars/1.jar");
	}

	public static void main(String[] args) {
		Ex0Wordcount wc = new Ex0Wordcount();
		wc.filterOnWordcount();

	}

	public JavaRDD<String> loadData() {

		JavaRDD<String> words = sc.textFile(pathToFile).flatMap(
				new FlatMapFunction<String, String>() {
					public Iterable call(String line) throws Exception {
						return Arrays.asList(line.split(" "));
					}
				});

		return words;

	}

	/**
	 * Now count how much each word appears!
	 */
	public JavaPairRDD<String, Integer> wordcount() {
		JavaRDD<String> words = loadData();

		// code here
		JavaPairRDD<String, Integer> couples = words
				.mapToPair(new PairFunction<String, String, Integer>() {

					@Override
					public Tuple2<String, Integer> call(String s)
							throws Exception {
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		// code here
		JavaPairRDD<String, Integer> result = couples
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer i0, Integer i1)
							throws Exception {
						return i0 + i1;
					}
				});

		return result;
	}

	/**
	 * Now keep the word which appear strictly more than 4 times!
	 */
	public JavaPairRDD<String, Integer> filterOnWordcount() {
		JavaPairRDD<String, Integer> wordcounts = wordcount();

		List<Tuple2<String, Integer>> output = wordcounts.collect();
		JavaPairRDD<String, Integer> filtered = null;
		// int count=0;
		for (Tuple2<?, ?> tuple : output) {
			if (Integer.parseInt(tuple._2() + "") > 4) {
				// filtered=tuple;
				System.out.println(tuple._1() + "==" + tuple._2());
			}

		}
		return filtered;
	}

}