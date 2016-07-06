package com.sparksqldemo

import org.apache.spark.sql.{SQLContext,Row}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 15-7-21.
 */
object SparkSqlDemo2 {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("sparksqldemo2").setMaster("local")
    val sc= new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val a="id name".split(" ")
    val schema1=StructType(StructField(a(0),IntegerType,true)::StructField(a(1),StringType,true)::Nil)
    val rowRDD1=sc.textFile(args(0)+"/2.txt").map(line=>{val t=line.split("-");Row(t(0).toInt,t(1))})
    sqlContext.createDataFrame(rowRDD1,schema1).registerTempTable("teacher")

    val b="stu_id tea_id score".split(" ")
    val schema2=StructType(StructField(b(0),IntegerType,true)::StructField(b(1),IntegerType,true)::StructField(b(2),DoubleType,true)::Nil)
    val rowRDD2=sc.textFile(args(0)+"/3.txt").map(line=>{val t=line.split("-");Row(t(0).toInt,t(1).toInt,t(2).toDouble)})
    sqlContext.createDataFrame(rowRDD2,schema2).registerTempTable("grade")

    sqlContext.sql("select avg(score),name from grade,teacher where teacher.id = grade.tea_id group by name").map(r=>(r.getAs[Double](0),r.getAs[String](1))).sortByKey(false).collect.foreach(println)
  }
}
