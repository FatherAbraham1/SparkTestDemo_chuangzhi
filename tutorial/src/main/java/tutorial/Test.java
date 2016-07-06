package tutorial;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("test").setMaster("spark://master:7077");		 
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
//		sc.addJar("/home/sun/jars/myjar.jar");
		//sc.addJar("/home/hadoop/tools/jars/1.jar");
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data);
		System.out.println(distData.count());
	}

}
