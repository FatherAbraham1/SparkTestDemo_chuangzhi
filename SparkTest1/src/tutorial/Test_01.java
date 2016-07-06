package tutorial;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Test_01 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("test").setMaster("spark://master:7077");		
		conf.set("spark.executor.memory", "3000m");
		@SuppressWarnings("resource")
	JavaSparkContext sc = new JavaSparkContext(conf);
//		sc.addJar("/home/sun/jars/myjar.jar");
		sc.addJar("/home/hadoop/tools/jars/1.jar");
		Ex0Wordcount wc=new Ex0Wordcount();
		wc.filterOnWordcount();
	}

}
