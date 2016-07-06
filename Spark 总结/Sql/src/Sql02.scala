import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lyen on 15-7-21.
 */
object Sql02 {
  //定义样例类，用于匹配
  case class Student(stu_id:Int,stu_name:String)
  case class Teacher(tec_id:Int,tec_name:String)
  case class Grade(stu_id:Int,tec_id:Int,score:Double)
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sql01").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //导入RDD到SchemaRDD的模式转换
    import sqlContext.implicits._
    val student = sc.textFile("/home/lyen/test2/1.txt").map(i=>{val t=i.split("-");Student(t(0).toInt,t(1))}).toDF()
    val teacher = sc.textFile("/home/lyen/test2/2.txt").map(i=>{val t=i.split("-");Teacher(t(0).toInt,t(1))}).toDF()
    val grade = sc.textFile("/home/lyen/test2/3.txt").map(i=>{val t=i.split("-");Grade(t(0).toInt,t(1).toInt,t(2).toDouble)}).toDF()
    //

    student.registerTempTable("student")
    teacher.registerTempTable("teacher")
    grade.registerTempTable("grade")
    val sql1 = sqlContext.sql("select tec_name,min(score) from teacher,grade where teacher.tec_id=grade.tec_id group by tec_name").map(i=>(i.getAs[String](0),i.getAs[Double](1)))
    val sql2 = sqlContext.sql("select tec_name,stu_name,score from teacher,student,grade where grade.stu_id=student.stu_id and grade.tec_id=teacher.tec_id group by stu_name,tec_name,score").map(i=>(i.getAs[String](0),(i.getAs[String](1),i.getAs[Double](2))))
    sql1.join(sql2.groupByKey()).map(i=>(i._1,i._2._1,i._2._2.filter(_._2==i._2._1).map(i=>(i._1)))).collect().foreach(println)


  }

}
