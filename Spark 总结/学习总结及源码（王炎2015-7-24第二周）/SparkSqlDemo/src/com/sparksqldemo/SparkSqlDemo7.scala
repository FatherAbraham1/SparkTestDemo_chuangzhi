package com.sparksqldemo


import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hadoop on 15-7-22.
 * 任务目的：在movie中找出指定的某种类型的电影，列出电影得分的平均分
 */
object SparkSqlDemo7 {
  case class Movie(id:Int,movie_name:String,movie_type:String)
  case class Grade(id:Int,movie_id:Int,score:Double,time:Int)
  case class User(id:Int,sex:String,age_id:Int,occupation_id:Int,post_id:String)
  case class Age(id:Int,age:String)
  case class Occupation(id:Int,occupation:String)
  def main(args:Array[String]): Unit ={
    /*if(args.length <1 ||args.length>2){
      print("输入参数不正确！")
    }*/
    val conf = new SparkConf().setAppName("sparksqlDemo7").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val movie =sc.textFile(args(0)+"/movies.dat").map(_.split("::")).map(m=>Movie(m(0).toInt,m(1),m(2))).toDF.explode("movie_type","type"){i:String=>i.split("\\|")}.drop("movie_type").show()
    //movie.registerTempTable("movie")
   /* val grade =sc.textFile(args(0)+"/ratings.dat").map(_.split("::")).map(g=>Grade(g(0).toInt,g(1).toInt,g(2).toDouble,g(3).toInt)).toDF()
    grade.registerTempTable("grade")
    val user =sc.textFile(args(0)+"/users.dat").map(_.split("::")).map(u=>User(u(0).toInt,u(1),u(2).toInt,u(3).toInt,u(4))).toDF()
    user.registerTempTable("user")
    val age =sc.textFile(args(0)+"/age").map(_.split(":")).map(a=>Age(a(0).toInt,a(1))).toDF()
    age.registerTempTable("age")
    val occupation =sc.textFile(args(0)+"/Occupation").map(_.split(":")).map(o=>Occupation(o(0).toInt,o(1))).toDF()
    occupation.registerTempTable("occupation")
    val a=sqlContext.sql("select age from user,age where age.id=user.age_id and user.id='"+args(1)+"'").map(_.getAs[String](0))
    val movietype=sqlContext.sql("select movie_type from movie where movie.id='"+args(2)+"'").map(_.getAs[String](0))
    val user_id=sqlContext.sql("select movie_name,score from user,age,grade,movie where age.id=user.age_id and user.id=grade.id and grade.movie_id=movie.id and age.age='"+a.first()+"' and movie_type='"+movietype.first()+"' group by movie_name,score").map(i=>(i.getAs[String](0),i.getAs[Double](1))).groupByKey().map(i=>(i._1,i._2.max/i._2.size))
   //  val result=sqlContext.sql("select grade.id,movie_name,score from grade,movie where grade.movie_id=movie.id and movie_type='"+movietype.first()+"' group by grade.id,movie_name,score").map(i=>(i.getAs[Int](0),i.getAs[Int](1),i.getAs[Double](2))).filter(_._1 == user_id).map(i=>(i._1,i._2,i._3))
    user_id.collect.foreach(println)*/
  }
}
