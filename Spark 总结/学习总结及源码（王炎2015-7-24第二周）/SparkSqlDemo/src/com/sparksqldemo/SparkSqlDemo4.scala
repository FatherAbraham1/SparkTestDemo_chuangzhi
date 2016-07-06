package com.sparksqldemo

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 15-7-21.
  */
object SparkSqlDemo4 {
   def main(args:Array[String]): Unit ={
     val conf = new SparkConf().setAppName("sparksqldemo4").setMaster("local")
     val sc= new SparkContext(conf)
     val sqlContext = new SQLContext(sc)

     val a="id name".split(" ")
     val schema1=StructType(StructField(a(0),IntegerType,true)::StructField(a(1),StringType,true)::Nil)
     val rowRDD1=sc.textFile(args(0)+"/1.txt").map(line=>{val t=line.split("-");Row(t(0).toInt,t(1))})
     sqlContext.createDataFrame(rowRDD1,schema1).registerTempTable("student")

     val b="id name".split(" ")
     val schema2=StructType(StructField(b(0),IntegerType,true)::StructField(b(1),StringType,true)::Nil)
     val rowRDD2=sc.textFile(args(0)+"/2.txt").map(line=>{val t=line.split("-");Row(t(0).toInt,t(1))})
     sqlContext.createDataFrame(rowRDD2,schema2).registerTempTable("teacher")

     val c="stu_id tea_id score".split(" ")
     val schema3=StructType(StructField(c(0),IntegerType,true)::StructField(c(1),IntegerType,true)::StructField(c(2),DoubleType,true)::Nil)
     val rowRDD3=sc.textFile(args(0)+"/3.txt").map(line=>{val t=line.split("-");Row(t(0).toInt,t(1).toInt,t(2).toDouble)})
     sqlContext.createDataFrame(rowRDD3,schema3).registerTempTable("grade")

     val result1=sqlContext.sql("select teacher.name,student.name,score from student,grade,teacher where teacher.id=grade.tea_id and student.id=grade.stu_id group by teacher.name,student.name,score").map(r=>(r.getAs[String](0),(r.getAs[String](1),r.getAs[Double](2)))).groupByKey()
     val result2=sqlContext.sql("select teacher.name,min(score) from teacher,grade where teacher.id=grade.tea_id group by teacher.name").map(r=>(r.getAs[String](0),r.getAs[String](1))).groupByKey()
     result1.join(result2).map(i=>(i._1,i._2._2.toBuffer(0),i._2._1.filter(_._2 == i._2._2.toBuffer(0)))).collect().foreach(println)

   }
 }
