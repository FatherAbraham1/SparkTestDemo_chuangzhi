import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lyen on 15-7-22.
 */
object Sql3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("demo2").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val schemeString = "stu_id tec_id score"
    val scheme = StructType(schemeString.split(" ").map(filedName => StructField(filedName, StringType, true)))
    val schemeRdd = sc.textFile("/home/lyen/test2/3.txt").map(line => {
      val r = line.split("-");
      Row(r(0), r(1), r(2))
    })
    val schemeString1 = "tec_id tec_name"
    val scheme1 = StructType(schemeString1.split(" ").map(filedName => StructField(filedName,StringType,true)))
    val schemeRdd1 = sc.textFile("/home/lyen/test2/2.txt").map(line=>{
      val r = line.split("-")
      Row(r(0),r(1))
    })
    sqlContext.createDataFrame(schemeRdd1,scheme1).registerTempTable("teacher")
    sqlContext.createDataFrame(schemeRdd, scheme).registerTempTable("grade")
    sqlContext.sql("select tec_name,avg(score) from teacher,grade where grade.tec_id = teacher.tec_id group by tec_name").map(r => (r(0), r(1))).collect.foreach(println)
  }

  }
