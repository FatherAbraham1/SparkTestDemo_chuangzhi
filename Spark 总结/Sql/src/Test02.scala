import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by lyen on 15-7-20.
 */
object Test02 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local")
    val sc = new SparkContext(conf)
    val student = sc.textFile("/home/lyen/test2/1.txt").map(i=>{val t=i.split("-");(t(0),t(1))})
    val teacher = sc.textFile("/home/lyen/test2/2.txt").map(i=>{val t=i.split("-");(t(0),t(1))})
    val combine = sc.textFile("/home/lyen/test2/3.txt").map(i=>{val t =i.split("-");(t(0),(t(1),t(2).toDouble))})
    val t = teacher.join(student.join(combine).map(_._2).map(i=>(i._2._1,(i._2._2,i._1)))).map(_._2)
    t.foreach(println)
    val t1 = t.groupByKey()
    t1.foreach(println)
    val t2 = t.map(i=>(i._1,i._2._1)).groupByKey().map(i=>(i._1,i._2.min))
    t1.join(t2).map(i=>(i._1,i._2._2,i._2._1.filter(_._1==i._2._2).map(_._2))).collect().foreach(println)


  }

}
