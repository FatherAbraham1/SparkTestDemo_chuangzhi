


import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by kexu on 15-7-21.
 */
object SQLTest {
  case class Teacher(id:Int,name:String)
  case class Student(id:Int,name:String)
  case class Grade(stu_id:Int,tec_id:Int,score:Double)
  def main(args: Array[String]) {
    if (args.length < 0) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("SQLTest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val student=sc.textFile(args(0)+"/1.txt").map(_.split("-")).map(w=>Student(w(0).toInt, w(1))).toDF()
    val teacher = sc.textFile(args(0)+"/2.txt").map(_.split("-")).map(i => Teacher(i(0).toInt, i(1))).toDF()
    val grade = sc.textFile(args(0)+"/3.txt").map(_.split("-")).map(i => Grade(i(0).toInt, i(1).toInt,i(2).toDouble)).toDF()
    student.registerTempTable("student")
    teacher.registerTempTable("teacher")
    grade.registerTempTable("grade")
    val result=sqlContext.sql("select teacher.name,avg(grade.score) scores from teacher,grade where teacher.id=grade.tec_id group by teacher.name order by scores desc")
    .map(f=>(f.getAs[Int](0),f.getAs[String](1))).collect().foreach(println)



  }
}
