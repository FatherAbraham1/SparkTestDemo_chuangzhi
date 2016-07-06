import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
/**
 * Created by kexu on 15-7-21.
 */
object SQLTest2 {

  case class Teacher(id: Int, name: String)

  case class Student(id: Int, name: String)

  case class Grade(stu_id: Int, tec_id: Int, score: Double)

  def main(args: Array[String]) {
    if (args.length < 0) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("SQLTest2").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
   /* val student = sc.textFile(args(0) + "/1.txt").map(_.split("-")).map(w => Student(w(0).toInt, w(1)))
    val teacher = sc.textFile(args(0) + "/2.txt").map(_.split("-")).map(i => Teacher(i(0).toInt, i(1)))
    val grade = sc.textFile(args(0) + "/3.txt").map(_.split("-")).map(i => Grade(i(0).toInt, i(1).toInt, i(2).toDouble))
   */
   val student = sc.textFile(args(0) + "/1.txt").map(_.split("-")).map(w =>Row(w(0).toInt, w(1)))
    val teacher = sc.textFile(args(0) + "/2.txt").map(_.split("-")).map(i => Row(i(0).toInt, i(1)))
    val grade = sc.textFile(args(0) + "/3.txt").map(_.split("-")).map(i => Row(i(0).toInt, i(1).toInt, i(2).toDouble))

    /*student.toDF().registerTempTable("student")
    teacher.toDF().registerTempTable("teacher")
    grade.toDF().registerTempTable("grade")*/

    val TeaSchame=StructType(StructField("id",IntegerType,true)::StructField("name",StringType,true)::Nil)
    val StuSchame=StructType(StructField("id",IntegerType,true)::StructField("name",StringType,true)::Nil)
    val GraSchame=StructType(StructField("stu_id",IntegerType,true)::StructField("tec_id",IntegerType,true)::StructField("score",DoubleType,true)::Nil)
    /*val schemaString = "stu_id tec_id grade"
    val schame=StructType(schemaString.split(" ").map(filedName=>StructField(filedName,StringType,true)))*/
    sqlContext.createDataFrame(student,StuSchame).registerTempTable("student")
    sqlContext.createDataFrame(teacher,TeaSchame).registerTempTable("teacher")
    sqlContext.createDataFrame(grade,GraSchame).registerTempTable("grade")




    val re1 = sqlContext.sql("select teacher.name,max(grade.score) from teacher,grade where teacher.id=grade.tec_id group by teacher.name ")
      .map(f => (f.getAs[String](0), f.getAs[Double](1)))
    val re2=sqlContext.sql("select teacher.name,student.name,grade.score from teacher,grade,student where teacher.id=grade.tec_id and student.id=" +
      "grade.stu_id").map(i=>(i.getAs[String](0),(i.getAs[String](1),i.getAs[Double](2)))).groupByKey().join(re1).map(y=>(y._1,y._2._2,y._2._1.filter(_._2==y._2._2).map(_._1))).collect()
    .foreach(println)

  }
}
