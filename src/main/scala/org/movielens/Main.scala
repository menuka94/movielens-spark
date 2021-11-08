package org.movielens

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val question: Integer = args(0).toInt
    println(s"SPARK_MASTER: ${Constants.SPARK_MASTER}")
    if (Constants.USE_HDFS) {
      println("Using HDFS")
    } else {
      println("Using local storage")
      println(s"DATA_DIR: ${Constants.DATA_DIR}")
    }

    val conf: SparkConf = new SparkConf()
      .setMaster(Constants.SPARK_MASTER)
      .setAppName(s"MovieLens - Q${question}")

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    question.toInt match {
      case 1 => new Q1(spark).run()
      case 2 => new Q2(spark).run()
      case 3 => new Q3(spark).run()
      case 4 => new Q4(spark).run()
      case 5 => new Q5(spark).run()
      case 6 => new Q6(spark).run()
    }
  }
}
