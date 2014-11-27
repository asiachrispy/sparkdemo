package com.lietou.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
 * Description : some words
 * Author: chris 
 * Date: 2014/10/22
 */
object SQLJson {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "SQLJson", System.getenv("SPARK_HOME"))

    val jsonStrRDD = sc.parallelize(List( """{"name":"chris","age":20}""", """{"name":"asia","age":22}""",
      """{"name":"manda","age":19}""", """{"name":"joy","age":10}""",
      """{"name":"cici","age":15}""", """{"name":"pypy","age":30}"""))
    val sqlContext = new SQLContext(sc)
    val people = sqlContext.jsonRDD(jsonStrRDD)
//        sqlContext.jsonFile("resources/json/people.json") // or from json file
    people.registerTempTable("people")

    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 10 AND age <= 20")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
  }
}
