package org.movielens

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    println(s"SPARK_MASTER: ${Constants.SPARK_MASTER}")
    if (Constants.USE_HDFS) {
      println("Using HDFS")
    } else {
      println("Using local storage")
      println(s"DATA_DIR: ${Constants.DATA_DIR}")
    }

    val conf: SparkConf = new SparkConf()
      .setMaster(Constants.SPARK_MASTER)
      .setAppName("MovieLens-Spark")

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val q: Question = new Q5(spark)
    q.run()
  }
}
