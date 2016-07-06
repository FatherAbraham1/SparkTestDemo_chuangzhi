import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
/**
 * Created by kexu on 15-7-21.
 */
object DataFrameTest {
  case class user(u_id:Int,sex:String,age:String,occ:String,zc:String)
  case class age(a_id:String,a_value:String)
  case class occupation(o_id:String,o_value:String)
  case class movie(m_id:Int,title:String,genre:String)
  case class rating(u_id:Int,m_id:Int,rats:Double,time:String)
  def main(args: Array[String]) {
    if (args.length < 0) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("DataFrameTest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val genre=args(1)
    println(genre)
    import sqlContext.implicits._
    sc.textFile(args(0) + "/users.dat").map(_.split("::")).map(w =>user(w(0).toInt,w(1),w(2),w(3),w(4))).toDF().registerTempTable("user")
    sc.textFile(args(0)+"/age").map(_.split(":")).map(a=>age(a(0),a(1))).toDF().registerTempTable("age")
    sc.textFile(args(0)+"/Occupation").map(_.split(":")).map(a=>occupation(a(0),a(1))).toDF().registerTempTable("occupation")
    sc.textFile(args(0)+"/movies.dat").map(_.split("::")).map(a=>movie(a(0).toInt,a(1),a(2))).toDF().explode("genre","genre2"){e:String=>e.split("\\|")}.drop("genre").withColumnRenamed("genre2","genre").registerTempTable("movie")
    sc.textFile(args(0)+"/ratings.dat").map(_.split("::")).map(a=>rating(a(0).toInt,a(1).toInt,a(2).toDouble,a(3))).toDF().registerTempTable("rating")
    val re1=sqlContext.sql("select user.u_id,user.sex,age.a_value,occupation.o_value,user.zc from user,age,occupation where user.age=age.a_id and user.occ=occupation.o_id")
      .map(f => (f.getAs[Int](0), f.getAs[String](1),f.getAs[String](2),f.getAs[String](3),f.getAs[String](4)))//.collect().foreach(println)

    val re3=sqlContext.sql("select movie.m_id,avg(rating.rats) from movie,rating where movie.m_id=rating.m_id and genre='"+genre+"'  group by movie.m_id")
     .map(f=>(f.getAs[Int](0),f.getAs[Double](1))).toDF("m_id","maxrats").registerTempTable("table1")
    val re4=sqlContext.sql("select movie.title,table1.maxrats from movie,table1 where movie.m_id=table1.m_id order by table1.maxrats desc").map(a=>(a.getAs[String](0),a.getAs[Double](1).toString)).take(20)

    val age_ =sqlContext.sql("select user.age from user where  user.u_id="+args(1).toInt).map((_.get(0))).collect()(0)
    val gen_ =sqlContext.sql("select movie.genre from movie where movie.m_id="+args(2).toInt).map((_.get(0))).collect()(0)

    val re5=sqlContext.sql("select movie.m_id,avg(rating.rats) from movie,rating where movie.m_id=rating.m_id and genre='"+gen_ +"' group by movie.m_id").map(a=>(a.getAs[Int](0),a.getAs[Double](1)))
    val re7=sqlContext.sql("select rating.m_id,user.u_id from rating,user where rating.u_id=user.u_id and user.age='"+age_ +"'").map(a=>(a.getAs[Int](0),a.getAs[Int](1))).join(re5).map(a=>(a._1,a._2._2)).distinct().toDF("mid","fen").registerTempTable("table4")//.collect().foreach(println)
    val re8=sqlContext.sql("select movie.title,table4.fen from movie,table4 where table4.mid=movie.m_id order by table4.fen desc").map(a=>(a.getAs[String](0),a.getAs[Double](1))).distinct().toDF("qq","yy").orderBy("yy").collect().foreach(println)









  }
}