package com.sparksqldemo

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hadoop on 15-7-21.
 */

object SparkSqlDemo3 {
     //定义样例类，用于匹配
    case class Student(id:Int,name:String)
    case class Teacher(id:Int,name:String)
    case class Grade(stu_id:Int,tea_id:Int,score:Double)

    def main(args:Array[String]): Unit ={
      val conf = new SparkConf().setAppName("sparksqldemo3").setMaster("local")
      val sc= new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      // this is used to implicitly convert an RDD to a DataFrame.
      import sqlContext.implicits._
      //toDF方法，转换为一个DataFrame
      // Create an RDD of Person objects and register it as a table.
      val student=sc.textFile(args(0)+"/1.txt").map(_.split("-")).map(s=>Student(s(0).toInt,s(1))).toDF()
      student.registerTempTable("student")
      val teacher=sc.textFile(args(0)+"/2.txt").map(_.split("-")).map(t=>Teacher(t(0).toInt,t(1))).toDF()
      teacher.registerTempTable("teacher")
      val grade=sc.textFile(args(0)+"/3.txt").map(_.split("-")).map(g=>Grade(g(0).toInt,g(1).toInt,g(2).toDouble)).toDF()
      grade.registerTempTable("grade")
      val result1=sqlContext.sql("select teacher.name,student.name,score from student,grade,teacher where teacher.id=grade.tea_id and student.id=grade.stu_id group by teacher.name,student.name,score").map(r=>(r.getAs[String](0),(r.getAs[String](1),r.getAs[Double](2)))).groupByKey()
      val result2=sqlContext.sql("select teacher.name,min(score) from teacher,grade where teacher.id=grade.tea_id group by teacher.name").map(r=>(r.getAs[String](0),r.getAs[String](1))).groupByKey()
      result1.join(result2).map(i=>(i._1,i._2._2.toBuffer(0),i._2._1.filter(_._2 == i._2._2.toBuffer(0)))).collect().foreach(println)
    }
}
