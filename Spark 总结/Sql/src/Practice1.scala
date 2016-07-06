import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lyen on 15-7-22.
 */
case class Users(user_id:Int,user_sex:String,age_id:Int,occu_id:Int,email:String)
case class Age(age_id:Int,age_between:String)
case class Occupation(occu_id:Int,occupation:String)
object Practice01 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("movies").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val users = sc.textFile("/home/lyen/datatest/users.dat").map(i=>{val tmp=i.split("::");Users(tmp(0).toInt,tmp(1),tmp(2).toInt,tmp(3).toInt,tmp(4))}).toDF()
    val age = sc.textFile("/home/lyen/datatest/age").map(i=>{val tmp = i.split(":");Age(tmp(0).toInt,tmp(1))}).toDF()
    val occu = sc.textFile("/home/lyen/datatest/Occupation").map(i=>{val tmp = i.split(":");Occupation(tmp(0).toInt,tmp(1))}).toDF()
    users.registerTempTable("users")
    age.registerTempTable("age")
    occu.registerTempTable("occu")
    val firstOpration = sqlContext.sql("select user_id,user_sex,age_between,occupation,email from users,occu,age where users.age_id=age.age_id and users.occu_id=occu.occu_id group by user_id,user_sex,age_between,occupation,email").map(i=>(
      (i.getAs[Int](0),i.getAs[String](1),i.getAs[Int](2),i.getAs[String](3),i.getAs[String](4))
      ))
    firstOpration.collect().foreach(println)

  }

}
