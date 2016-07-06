package tutorial;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;

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
public class Ex0Wordcount {

  private static String pathToFile = "data/wordcount.txt";

  public JavaRDD<String> loadData() {
    SparkConf conf = new SparkConf()
        .setAppName("Wordcount")
        .set("spark.driver.allowMultipleContexts", "true")
        .setMaster("spark://op3:8888");
      //  .setMaster("local[*]"); // here local mode. And * means you will use as much as you have cores.

    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> words = sc.textFile(pathToFile).flatMap(new FlatMapFunction<String, String>(){
		public Iterable call(String line) throws Exception {
			return Arrays.asList( line.split(" ")) ;
		}
    });

    return words;

  }

  /**
   *  Now count how much each word appears!
   */
  public JavaPairRDD<String, Integer> wordcount() {
    JavaRDD<String> words = loadData();
    
    // code here
    JavaPairRDD<String, Integer> couples = null;

    // code here
    JavaPairRDD<String, Integer> result = null;

    return result;
  }

  /**
   *  Now keep the word which appear strictly more than 4 times!
   */
  public JavaPairRDD<String, Integer> filterOnWordcount() {
    JavaPairRDD<String, Integer> wordcounts = wordcount();

    // TODO write code here
    JavaPairRDD<String, Integer> filtered = null;

    return filtered;

  }

  
}