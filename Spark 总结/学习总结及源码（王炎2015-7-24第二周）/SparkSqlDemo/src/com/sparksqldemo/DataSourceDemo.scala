package com.sparksqldemo

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 15-7-21.
  */
object DataSourceDemo {
   def main(args:Array[String]): Unit ={
     val conf = new SparkConf().setAppName("datasourcedemo").setMaster("local")
     val sc= new SparkContext(conf)
     val sqlContext = new SQLContext(sc)
     import sqlContext.implicits._
     /*
        在官方文档上读取数据库连接，发现SQLContxt中的load方法已经过时
        连接jdbc数据库的方法全都被封装在DataFrameReader中,SQLContxt中调用read()方法可以得到该类的对象
        DataFrameReader中有jdbc()的方法，最基本的有三个参数
        url:连接数据库的地址
        table:数据库中表的名字
        Properties:其他的参数设置，例如用户名、密码等可以在Properties中设置
      */
     //第一种方式
     val p= new Properties()
     p.put("user","root")
     val c1=sqlContext.read.format("jdbc").jdbc("jdbc:mysql://localhost:3306/test","student",p)
     //第二种方式
     val c2=sqlContext.read.format("jdbc").jdbc("jdbc:mysql://localhost:3306/test?user=root","student",new Properties())
     //c1.select("name").show()
    // c2.select("id","name","score").show()
     //c2.columns.foreach(println)
    // c2.select("score").show
     c2.filter($"score">66.0).show//filter($"score">66.0).show()
   }
 }
