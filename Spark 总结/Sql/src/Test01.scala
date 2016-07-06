import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
/**
 * Created by lyen on 15-7-20.
 */
//
object Test01 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)
    val t = sc.textFile("/home/lyen/test2/3.txt").map(i=>{val t=i.split("-");(t(1).toInt,t(2).toDouble)}).groupByKey().map(i=>(i._1,i._2.sum/i._2.size))
    val t1=  sc.textFile("/home/lyen/test2/2.txt").map(i=>{val t=i.split("-");(t(0).toInt,t(1))})
    t.join(t1).map((_._2)).sortByKey(false).groupByKey().foreach(println)
  }

}
