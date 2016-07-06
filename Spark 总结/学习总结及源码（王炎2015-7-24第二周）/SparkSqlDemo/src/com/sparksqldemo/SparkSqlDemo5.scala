package com.sparksqldemo

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hadoop on 15-7-22.
 * 任务目的：将user中年龄编号和职业比编号转换为对应的年龄和职业
 */
object SparkSqlDemo5 {
  case class User(id:Int,sex:String,age_id:Int,occupation_id:Int,post_id:String)
  case class Age(id:Int,age:String)
  case class Occupation(id:Int,occupation:String)
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("sparksqlDemo5").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val user =sc.textFile(args(0)+"/users.dat").map(_.split("::")).map(u=>User(u(0).toInt,u(1),u(2).toInt,u(3).toInt,u(4))).toDF()
    user.registerTempTable("user")
    val age =sc.textFile(args(0)+"/age").map(_.split(":")).map(a=>Age(a(0).toInt,a(1))).toDF()
    age.registerTempTable("age")
    val occupation =sc.textFile(args(0)+"/Occupation").map(_.split(":")).map(o=>Occupation(o(0).toInt,o(1))).toDF()
    occupation.registerTempTable("occupation")
    sqlContext.sql("select user.id,sex,age,occupation,post_id from user,age,occupation where user.age_id=age.id and user.occupation_id=occupation.id group by user.id,sex,age,occupation,post_id").map(i=>(i.getAs[Int](0),i.getAs[String](1),i.getAs[String](2),i.getAs[String](3),i.getAs[String](4))).take(20).foreach(println)
  }
}
