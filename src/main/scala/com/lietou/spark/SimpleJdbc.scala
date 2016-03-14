/**
 * Illustrates loading data over JDBC
 */
package com.lietou.spark

import org.apache.spark._
import org.apache.spark.rdd.JdbcRDD
import java.sql.{DriverManager, ResultSet}

object SimpleJdbc {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "SimpleJdbc", System.getenv("SPARK_HOME"))
    val data = new JdbcRDD(sc,
      createConnection, "SELECT * FROM rs_job2job_txt WHERE ? <= id AND ID <= ?",
      lowerBound = 1, upperBound = 10, numPartitions = 2, mapRow = extractValues)
    println(data.collect().toList)
  }

  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    DriverManager.getConnection("")
  }

  def extractValues(r: ResultSet) = {
    (r.getInt(1), r.getString(2))
  }
}
