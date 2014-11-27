package com.lietou.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Description : some words
 * Author: chris 
 * Date: 2014/9/4
 */

/**
 *
 * 参数可以通过sqlContext来设置：
 *  spark.sql.parquet.binaryAsString true|false 是否区分二进制文件和文本文件
 *  spark.sql.parquet.cacheMetadata true|false 是否开启元数据缓存，课加速查询
 *  spark.sql.parquet.compression.codec snappy|gzip|lzo|uncompressed 压缩方式
 */
object SQLParquet {
  case class Person(name: String, age: Int)

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "SQLParquet", System.getenv("SPARK_HOME"))
    val people = sc.textFile("hdfs://name1:9000/test/people/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))

    val sqlContext = new SQLContext(sc)
    import sqlContext._
    people.saveAsParquetFile("people.parquet")
    val parquetFile = sqlContext.parquetFile("people.parquet")

    parquetFile.registerTempTable("people")
    val teenagers = sql("SELECT name FROM people WHERE age >= 10 AND age <= 20")
    teenagers.collect().foreach(println)

  }
}
