package demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by kexu on 15-7-20.
 */
object scoreList2 {


  def main(args: Array[String]) {
    if (args.length < 0) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("scoreList2").setMaster("local")
    val sc = new SparkContext(conf)
    val linescore = sc.textFile(args(0)+"/3.txt").map(_.split("-")).map(i=>(i(0),(i(1),i(2).toDouble)))//将文件3中的数据转换为RDD
    linescore.cache()
    val lineteacher=sc.textFile(args(0)+"/2.txt").map(_.split("-")).map(i=>(i(0),i(1)))//将文件2中的数据转换为RDD

    val linestudent=sc.textFile(args(0)+"/1.txt").map(_.split("-")).map(i=>(i(0),i(1))).join(linescore)
      .map(u=>((u._2)._1,(u._2)._2)).map(o=>((o._2)._1,(o._1,(o._2)._2))).join(lineteacher).map(y=>((y._2)._2,(y._2)._1))

    val y1=linestudent.map(r=>(r._1,(r._2)._2)).groupByKey().map(o=>(o._1,o._2.max))
    val m1=linestudent.groupByKey().join(y1).map(t=>(t._1,t._2._2,t._2._1.filter(_._2==t._2._2).map(_._1))).map(i=>(i._1,(i._2,i._3)))
    val y2=linestudent.map(r=>(r._1,(r._2)._2)).groupByKey().map(o=>(o._1,o._2.min))
    val m2=linestudent.groupByKey().join(y2).map(t=>(t._1,t._2._2,t._2._1.filter(_._2==t._2._2).map(_._1))).map(i=>(i._1,(i._2,i._3))).join(m1)
      .map(i=>(i._1,("最低分："+i._2._1._1,("学生列表"+i._2._1._2).replace("ArrayBuffer","")),("最高分："+i._2._2._1,("学生列表"+i._2._2._2).replace("ArrayBuffer","")))).foreach(println)



  }

}
