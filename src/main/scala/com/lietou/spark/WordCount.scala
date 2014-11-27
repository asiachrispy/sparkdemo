package com.lietou.spark

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "WordCount", System.getenv("SPARK_HOME"))
    val input = sc.textFile(args(1)) // 读入文件内容
    val words = input.flatMap(_.split(" ")) // 合并文件内容，然后按行输出，对行内容进行分词
    val counts = words.map((_, 1)).reduceByKey(_ + _) // map阶段单词统计，reduce阶段单词汇总
    counts.saveAsTextFile(args(2))
  }
}

// 最简单的业务代码可以使用一行完成
// sc.textFile(args(1)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile(args(2))