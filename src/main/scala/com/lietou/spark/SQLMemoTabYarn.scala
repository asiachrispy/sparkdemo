package com.lietou.spark

import org.apache.spark._
import org.apache.spark.sql._

/**
 * 内存表：
 */
object SQLMemoTabYarn {

  case class Person(name: String, age: Int)

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("SQLMemoTabYarn")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.createSchemaRDD

    // for case class
    val people = sc.textFile("hdfs://name1:9000/test/people/people.txt") // 读入文件内容
    people.map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).registerTempTable("people")
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 10 AND age <= 20")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
  }
}
