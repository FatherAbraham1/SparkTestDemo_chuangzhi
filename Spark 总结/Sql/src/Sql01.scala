import org.apache.spark.sql.{SQLContext,DataFrame}
import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by lyen on 15-7-21.
 */
object Sql01 {
  //定义样例类，用于匹配
  case class Student(stu_id:Int,stu_Name:String)
  case class Teacher(tec_id:Int,tec_name:String)
  case class Grade(stu_id:Int,tec_id:Int,score:Double)
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sql01").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //这是用于隐式转换一个RDD改为DataFrame
    import sqlContext.implicits._

    val student = sc.textFile("/home/lyen/test2/1.txt").map(i=>{val t=i.split("-");Student(t(0).toInt,t(1))}).toDF()
    student.registerTempTable("student")
    val teacher = sc.textFile("/home/lyen/test2/2.txt").map(i=>{val t=i.split("-");Teacher(t(0).toInt,t(1))}).toDF()
    teacher.registerTempTable("teacher")
    val grade = sc.textFile("/home/lyen/test2/3.txt").map(i=>{val t=i.split("-");Grade(t(0).toInt,t(1).toInt,t(2).toDouble)}).toDF()
    grade.registerTempTable("grade")
    // 单表查询
    val sql1 = sqlContext.sql("select tec_id,avg(score) from grade group by tec_id").map(i=>(i.getAs[Int](0),i.getAs[Double](1)))
    val sql2 = sqlContext.sql("select tec_id,tec_name from teacher").map(i=>(i.getAs[Int](0),i.getAs[String](1)))
    sql1.join(sql2).map(_._2).groupByKey().collect().foreach(println)
    //多表查询
    val sql0 = sqlContext.sql("select tec_name,avg(score) from teacher,grade where grade.tec_id = teacher.tec_id group by tec_name").map(i=>(i.getAs[String](0),i.getAs[Double](1)))
    sql0.collect().foreach(println)

  }


}
