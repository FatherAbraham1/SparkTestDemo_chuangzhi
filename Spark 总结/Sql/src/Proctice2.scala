import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lyen on 15-7-22.
 */
//根据指定电影类型求出该电影的平均评分再排序
object Proctice2 {
  case class Movie(movie_id:Int,movie_name:String,movie_type:String)
  case class Ratings(user_id:Int,movie_id:Int,score:Double,runtime:String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("movies").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val movie_type = "Action"
    val movie = sc.textFile("/home/lyen/datatest/movies.dat").map(i=>{val tmp=i.split("::");Movie(tmp(0).toInt,tmp(1),tmp(2))}).toDF()
    val ratings = sc.textFile("/home/lyen/datatest/ratings.dat").map(i=>{val tmp = i.split("::");Ratings(tmp(0).toInt,tmp(1).toInt,tmp(2).toDouble,tmp(3))}).toDF()
    movie.registerTempTable("movies")
    ratings.registerTempTable("ratings")
    /*
    val firstOperation = sqlContext.sql("select movie_id,movie_name from movies where movie_type='"+movie_type+"'")
    val seconOPeration = sqlContext.sql("select movie_id,avg(score) from ratings group by movie_id")
    firstOperation.join(seconOPeration,firstOperation("movie_id")===seconOPeration("movie_id")).drop("movie_id").sort(firstOperation("c1").desc).show()
    */
    val firstOperation = sqlContext.sql("select movie_id,movie_name from movies where movie_type='"+movie_type+"'").map(i=>(i.getAs[Int](0),i.getAs[String](1)))
    val seconOPeration = sqlContext.sql("select movie_id,avg(score) from ratings group by movie_id").map(i=>(i.getAs[Int](0),i.getAs[Double](1)))
    seconOPeration.join(firstOperation).map(_._2).distinct().sortByKey(false).collect().foreach(println)

  }

}
