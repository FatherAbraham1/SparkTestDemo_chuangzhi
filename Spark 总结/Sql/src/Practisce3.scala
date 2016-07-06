import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lyen on 15-7-22.
 */
object Practisce3 {
  case class Users(user_id:Int,user_sex:String,age_id:Int,occu_id:Int,email:String)
  case class Age(age_id:Int,age_between:String)
  case class Movie(movie_id:Int,movie_name:String,movie_type:String)
  case class Ratings(user_id:Int,movie_id:Int,score:Double,runtime:String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("movies").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val userId:Int= 2
    val watchedMovie = "U.S. Marshalls (1998)"
    val users = sc.textFile("/home/lyen/datatest/users.dat").map(i=>{val tmp=i.split("::");Users(tmp(0).toInt,tmp(1),tmp(2).toInt,tmp(3).toInt,tmp(4))}).toDF()
    val movie = sc.textFile("/home/lyen/datatest/movies.dat").map(i=>{val tmp=i.split("::");Movie(tmp(0).toInt,tmp(1),tmp(2))}).toDF()
    val ratings = sc.textFile("/home/lyen/datatest/ratings.dat").map(i=>{val tmp = i.split("::");Ratings(tmp(0).toInt,tmp(1).toInt,tmp(2).toDouble,tmp(3))}).toDF()
    val age = sc.textFile("/home/lyen/datatest/age").map(i=>{val tmp = i.split(":");Age(tmp(0).toInt,tmp(1))}).toDF()

    users.registerTempTable("users")
    age.registerTempTable("age")
    movie.registerTempTable("movies")
    ratings.registerTempTable("ratings")

    val first = sqlContext.sql("select age_id from users where user_id='"+userId+"'").map((_.getAs[Int](0))).collect()(0)
    val second = sqlContext.sql("select user_id from users where age_id = '"+first+"'").map((_.getAs[Int](0))).collect()(0)
    val third = sqlContext.sql("select movie_type from movies where movie_name='"+ watchedMovie+"'").map((_.getAs[String](0))).collect()(0)
    val fourth = sqlContext.sql("select movie_name,avg(score) from movies,ratings where movies.movie_id=ratings.movie_id and user_id='"+second+"' and movie_type='"+third+"' group by movie_name")
    fourth.foreach(println)

  }



}
