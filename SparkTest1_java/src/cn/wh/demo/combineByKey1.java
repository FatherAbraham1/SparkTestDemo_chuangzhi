package cn.wh.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class combineByKey1 {
	
	public static void aggregateByKeyTest(JavaSparkContext spark){
        Tuple2<Integer,Integer> t1 = new Tuple2<Integer,Integer>(1, 3);
        Tuple2<Integer,Integer> t2 = new Tuple2<Integer,Integer>(1, 2);
        Tuple2<Integer,Integer> t3 = new Tuple2<Integer,Integer>(1, 4);
        Tuple2<Integer,Integer> t4 = new Tuple2<Integer,Integer>(2, 3);
        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
        System.out.println("原始数据如下:");
        list.add(t1);
        list.add(t2);
        list.add(t3);
        list.add(t4);
         
        list.forEach(t -> {System.out.println(t._1 + "," + t._2());});
        System.out.println("====================================");
         
        JavaPairRDD<Integer,Integer> rdd1 = spark.parallelizePairs(list);
        JavaPairRDD<Integer,Integer> result = rdd1.aggregateByKey(11, (a,b) ->{
                    System.out.println("seqOp:" + a +","+ b);
                    return Math.max(a, b);
                }, (a,b)->{
                    System.out.println("combOp:" + a +","+ b);
                    return a+b;
                });
        rdd1.foreach(System.out::println);
        System.out.println("聚合返回结果:"+result.collectAsMap()); 
    }
	
	public static void aggregateTest(JavaSparkContext spark){
        JavaRDD<Integer> rdd1 = spark.parallelize(Arrays.asList(1,2,3,4,5,6),2);
        int result = rdd1.aggregate(11, (a,b) ->{
                    System.out.println("seqOp:" + a +","+ b);
                    return Math.min(a, b);
                }, (a,b)->{
                    System.out.println("combOp:" + a +","+ b);
                    return a+b;
                });
        rdd1.foreach(System.out::println);
        System.out.println("聚合返回结果:"+result); 
    }
	
	public static void main(String[] args) {
		 SparkConf sparkConf = new SparkConf().setAppName("combineByKey1").setMaster("local");
		    JavaSparkContext sc = new JavaSparkContext(sparkConf);
		   // combineByKeyTest(sc);
		  //  aggregateTest(sc);
		   // cogroupTest(sc);
		  //  lookUpTest(sc);
		    aggregateByKeyTest(sc);
	}
	/**
     * combineByKey 函数测试
     * [combineByKey测试]返回结果:{2=[5, 3], 1=[4, 2, 3]}
     */
    public static void combineByKeyTest(JavaSparkContext spark){
        Tuple2<Integer,Integer> t1 = new Tuple2<Integer,Integer>(1, 3);
        Tuple2<Integer,Integer> t2 = new Tuple2<Integer,Integer>(1, 2);
        Tuple2<Integer,Integer> t3 = new Tuple2<Integer,Integer>(1, 4);
        Tuple2<Integer,Integer> t4 = new Tuple2<Integer,Integer>(2, 3);
        Tuple2<Integer,Integer> t5 = new Tuple2<Integer,Integer>(2, 3);
        Tuple2<Integer,Integer> t6 = new Tuple2<Integer,Integer>(2, 5);
        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
        System.out.println("[combineByKey测试]原始数据如下:");
        list.add(t1);
        list.add(t2);
        list.add(t3);
        list.add(t4);
        list.add(t5);
        list.add(t6);
         
        list.forEach(t -> {System.out.println(t._1 + "," + t._2());});
        System.out.println("====================================");
         
        JavaPairRDD<Integer,Integer> rdd1 = spark.parallelizePairs(list);
        JavaPairRDD<Integer,Set<Integer>> result = rdd1.combineByKey(v -> {
            // 如果集合不存在则创建集合
            Set<Integer> h = new HashSet<Integer>();
            h.add(v);
            return h;
        }, (h, v) -> {
            // 如果集合存在则添加数据
            h.add(v);
            return h;
        }, (vl, vr) -> {
            // 合并两个集合
            vl.addAll(vr);
            return vl;
        });
         
        rdd1.foreach(System.out::println);
        System.out.println("[combineByKey测试]返回结果:"+result.collectAsMap()); 
    }
     
    /**
     * cogroup函数原型一共有九个,  最多可以组合四个RDD。
     * 
     [测试 cogroup函数]原始数据如下<1>:
       1,3 | 1,2 | 2,4 | 2,3 | 2,3 | 
     [测试 cogroup函数]原始数据如下<2>:
      3,3 | 4,2 | 5,4 | 5,3 | ====================================
     [测试 cogroup函数]返回结果:[(4,([],[2])), (1,([3, 2],[])), (5,([],[4, 3])), (2,([4, 3, 3],[])), (3,([],[3]))]
     * @param spark
     */
    public static void cogroupTest(JavaSparkContext spark){
        Tuple2<Integer,Integer> t1 = new Tuple2<Integer,Integer>(1, 3);
        Tuple2<Integer,Integer> t2 = new Tuple2<Integer,Integer>(1, 2);
        Tuple2<Integer,Integer> t3 = new Tuple2<Integer,Integer>(2, 4);
        Tuple2<Integer,Integer> t4 = new Tuple2<Integer,Integer>(2, 3);
        Tuple2<Integer,Integer> t5 = new Tuple2<Integer,Integer>(2, 3);
        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
        System.out.println("[测试 cogroup函数]原始数据如下<1>:");
        list.add(t1);
        list.add(t2);
        list.add(t3);
        list.add(t4);
        list.add(t5);
         
        list.forEach(t -> {System.out.print(t._1 + "," + t._2() + " | ");});
         
        Tuple2<Integer,Integer> p1 = new Tuple2<Integer,Integer>(3, 3);
        Tuple2<Integer,Integer> p2 = new Tuple2<Integer,Integer>(4, 2);
        Tuple2<Integer,Integer> p3 = new Tuple2<Integer,Integer>(5, 4);
        Tuple2<Integer,Integer> p4 = new Tuple2<Integer,Integer>(5, 3);
        List<Tuple2<Integer,Integer>> listP = new ArrayList<Tuple2<Integer,Integer>>();
        System.out.println("[测试 cogroup函数]原始数据如下<2>:");
        listP.add(p1);
        listP.add(p2);
        listP.add(p3);
        listP.add(p4);
         
        listP.forEach(t -> {System.out.print(t._1 + "," + t._2() + " | ");});
        System.out.println("====================================");
        // 测试reduceByKey函数
        JavaPairRDD<Integer,Integer> rdd = spark.parallelizePairs(list);
        JavaPairRDD<Integer,Integer> rddp = spark.parallelizePairs(listP);
         
        List<Tuple2<Integer,Tuple2<Iterable<Integer>,Iterable<Integer>>>> result = rdd.cogroup(rddp).collect();
        System.out.println("[测试 cogroup函数]返回结果:"+result);
    }
     
     
    /**
     * lookUp 函数测试,就像使用 map的get方法一样，根据Key查询Value
     * [lookUp测试]返回结果:[3, 2, 4]
     */
    public static void lookUpTest(JavaSparkContext spark){
        Tuple2<Integer,Integer> t1 = new Tuple2<Integer,Integer>(1, 3);
        Tuple2<Integer,Integer> t2 = new Tuple2<Integer,Integer>(1, 2);
        Tuple2<Integer,Integer> t3 = new Tuple2<Integer,Integer>(1, 4);
        Tuple2<Integer,Integer> t4 = new Tuple2<Integer,Integer>(2, 3);
        Tuple2<Integer,Integer> t5 = new Tuple2<Integer,Integer>(2, 3);
        Tuple2<Integer,Integer> t6 = new Tuple2<Integer,Integer>(2, 5);
        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
        System.out.println("[lookUp测试]原始数据如下:");
        list.add(t1);
        list.add(t2);
        list.add(t3);
        list.add(t4);
        list.add(t5);
        list.add(t6);
         
        list.forEach(t -> {System.out.println(t._1 + "," + t._2());});
        System.out.println("====================================");
         
        JavaPairRDD<Integer,Integer> rdd1 = spark.parallelizePairs(list);
         
        List<Integer> result = rdd1.lookup(1);
        result.stream().forEach(System.out::println);
        System.out.println("[lookUp测试]返回结果:"+result); 
    }

}
