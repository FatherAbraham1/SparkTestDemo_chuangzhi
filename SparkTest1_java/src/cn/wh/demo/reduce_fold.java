package cn.wh.demo;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class reduce_fold {
	public static void main(String[] args) {
		 SparkConf sparkConf = new SparkConf().setAppName("reduce_fold").setMaster("local");
		    JavaSparkContext sc = new JavaSparkContext(sparkConf);
		    reduceTest(sc);
	}
	
	public static void reduceTest(JavaSparkContext spark){
        JavaRDD<Integer> rdd1 = spark.parallelize(Arrays.asList(1,2,3,4),5);
        // 测试reduce函数
        Integer result = rdd1.reduce((m,n) -> m+n);
        System.out.println("[测试 reduce函数]返回结果:"+result);
         
        JavaRDD<Integer> rddFold = spark.parallelize(Arrays.asList(1,2,3,4),6);
        int resultFold = rddFold.fold(11, (a,b)-> {
            System.out.println(a+"+"+b +"=" + (a+b));
            return a+b;
        });
        System.out.println("[测试 fold 函数]返回结果:"+resultFold);
    }

}
