package com.lietou.spark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

/**
 * Description : some words
 * Author: chris 
 * Date: 2014/10/28
 */
object TestKM {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "TestKM", System.getenv("SPARK_HOME"))

    val f = "file:///D:\\gitspace\\sparkwp\\data\\kmean_data.txt"
    val data = sc.textFile(f)

    val points = data.map(line => {
      val parts = line.split(" ").map(_.toDouble)
      Vectors.dense(parts)
      //(parts(0), parts(1))
    })

    val km = KMeans.train(points, 2, 5)
    println(km.clusterCenters.foreach(println))
  }
}
