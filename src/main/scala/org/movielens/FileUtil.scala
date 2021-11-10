package org.movielens

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileUtil {
  /**
   * Reads a CSV file from local disk or HDFS
   *
   * @param fileName Name of CSV file with extention
   * @param spark    SparkSession
   * @return CSV file as a Spark DataFrame
   */
  def readCsv(fileName: String, spark: SparkSession): DataFrame = {
    if (Constants.USE_HDFS) {
      val df: DataFrame = spark.read
        .option("header", "true")
        .csv(s"${Constants.HDFS_DATA_DIR}/$fileName")
      df
    } else {
      val df: DataFrame = spark.read
        .format("csv")
        .option("header", "true")
        .load(s"${Constants.LOCAL_DATA_DIR}/$fileName")
      df
    }
  }
}
