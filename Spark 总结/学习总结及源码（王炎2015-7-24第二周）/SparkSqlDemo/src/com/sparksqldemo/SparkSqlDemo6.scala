package com.sparksqldemo

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hadoop on 15-7-22.
 * 任务目的：在movie中找出指定的某种类型的电影，列出电影得分的平均分
 */
object SparkSqlDemo6 {
  case class Movie(id:Int,movie_name:String,movie_type:String)
  case class Grade(id:Int,movie_id:Int,score:Double,time:Int)
  def main(args:Array[String]): Unit ={
    if(args.length <1 ||args.length>2){
      print("输入参数不正确！")
    }
    val conf = new SparkConf().setAppName("sparksqlDemo6").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val movie =sc.textFile(args(0)+"/movies.dat").map(_.split("::")).map(m=>Movie(m(0).toInt,m(1),m(2))).toDF()
    movie.registerTempTable("movie")
    val grade =sc.textFile(args(0)+"/ratings.dat").map(_.split("::")).map(g=>Grade(g(0).toInt,g(1).toInt,g(2).toDouble,g(3).toInt)).toDF()
    grade.registerTempTable("grade")
    sqlContext.sql("select movie_type,movie_name,score from movie,grade where movie.id=grade.movie_id group by movie_type,movie_name,score").map(i=>(i.getAs[String](1),i.getAs[String](0),i.getAs[Double](2))).filter(_._2 == args(1)).map(i=>(i._1,i._3)).groupByKey().map(i=>(i._1,i._2.sum/i._2.size)).take(20).foreach(println)

  }
}
