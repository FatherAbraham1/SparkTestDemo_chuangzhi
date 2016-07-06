package com.sparksqldemo

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 15-7-21.
 */

object SparkSqlDemo1 {
     //定义样例类，用于匹配
   case class Student(id:Int,stu_name:String)
    case class Teacher(id:Int,tea_name:String)
    case class Grade(stu_id:Int,tea_id:Int,score:Double)

    def main(args:Array[String]): Unit = {
      val conf = new SparkConf().setAppName("sparksqldemo1").setMaster("local")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      // this is used to implicitly convert an RDD to a DataFrame.
      import sqlContext.implicits._
      //toDF方法，转换为一个DataFrame
      // Create an RDD of Person objects and register it as a table.
      val student = sc.textFile(args(0) + "/1.txt").map(_.split("-")).map(s => Student(s(0).toInt, s(1))).toDF()
      student.registerTempTable("student")
      val teacher = sc.textFile(args(0) + "/2.txt").map(_.split("-")).map(t => Teacher(t(0).toInt, t(1))).toDF()
      teacher.registerTempTable("teacher")
      val grade = sc.textFile(args(0) + "/3.txt").map(_.split("-")).map(g => Grade(g(0).toInt, g(1).toInt, g(2).toDouble)).toDF()
      grade.registerTempTable("grade")
     grade.join(student,grade("stu_id")===student("id")).join(teacher,grade("tea_id")===teacher("id")).drop("id").drop("stu_id").drop("tea_id").show()
      //grade.intersect(student).show()("a","id"){i:String=>(i.split("-")(0))}
    }
      //student.filter(student("id") > "2").show()
/*     def main(args:Array[String]): Unit ={
       val conf = new SparkConf().setAppName("sparksqldemo1").setMaster("local")
       val sc= new SparkContext(conf)
       val sqlContext = new SQLContext(sc)
       // this is used to implicitly convert an RDD to a DataFrame.
       import sqlContext.implicits._
       sc.textFile("/home/hadoop/test2/1.txt").toDF("a").explode("a","id"){i:String=>(Array(i.split("-")(0)))}.explode("a","name"){i:String=>(Array(i.split("-")(1)))}.drop("a").show()
    }*/
}
