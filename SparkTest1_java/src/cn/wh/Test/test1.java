package cn.wh.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class test1 implements Serializable{
	public static String path;
	static{
		path="/home/hadoop/data1/user-location-tim.txt";
		
	}
	public static  JavaPairRDD<String,  Iterable<String>> loadData(String path,JavaSparkContext sc) {


		JavaPairRDD<String, String> words = sc.textFile(path).flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			@Override
			public Iterable<Tuple2<String, String>> call(String line)
					throws Exception {
				String [] strs=line.split("\\|");
				String id=strs[0];
				String value=strs[1]+"|"+strs[2];
				java.util.List<Tuple2<String, String>> list=new ArrayList<Tuple2<String,String>>();
				list.add(new Tuple2<String, String>(id, value));
				
				return list;
			}
		});
		 JavaPairRDD<String,  Iterable<String>> group=words.groupByKey();
		return group;
	}
	
	


	
	public static void getUserLocationIndex(String path,JavaSparkContext sc){
		 JavaPairRDD<String,  Iterable<String>>  output = loadData(path,sc);
		//Iterator<String,  Iterable<String>> it2 = output.iterator(null, null);
		
//		while (it2.hasNext()) {
//			Tuple2<Integer, String> tuple2 = (Tuple2<Integer, String>) it2
//					.next();
//			System.out.println(tuple2._1() + "==" + tuple2._2());
//
//		}
//		
		
		
		
		
		
		
		
	}
	
	
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("test1").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    getUserLocationIndex(path,sc);
		
		
	}

}
