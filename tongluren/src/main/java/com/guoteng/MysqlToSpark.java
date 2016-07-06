package com.guoteng;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class MysqlToSpark implements Serializable{
	public static JavaPairRDD<String, String> getMysqlRDD(JavaSparkContext jsc,String table){
		String url = "jdbc:mysql://localhost:3306/test";
		Properties prop=new Properties();
		prop.setProperty("user", "root");
		prop.setProperty("password", "123456");
		
		SQLContext sqlContext=new SQLContext(jsc);
		DataFrame df =  sqlContext.read().jdbc(url, table, prop);
		JavaRDD<Row> rdd=df.javaRDD();
		JavaPairRDD<String, String> jpr=rdd.map(new Function<Row, String>() {

			@Override
			public String call(Row v1) throws Exception {
				// TODO Auto-generated method stub
				return v1.toString();
			}
		}).mapToPair(new PairFunction<String, String,String>() {

			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				// TODO Auto-generated method stub
				String begin=t.substring(1, t.indexOf("]"));
				String temp[]=begin.split(",");
				String key=temp[0];
				String value=temp[5];
				return new Tuple2<String, String>(key, value);
			}
		});
		return jpr;
}
}
