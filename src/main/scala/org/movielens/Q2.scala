package org.movielens

import org.apache.spark.sql.SparkSession

/**
 * What is the average number of genres for movies within the dataset?
 * @param spark: SparkSession
 */
class Q2 (spark: SparkSession) extends Question (spark){
  override def run(): Unit  ={
    println("Question 2")

    val moviesDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(s"${Constants.DATA_DIR}/movie.csv")

    println(s"No. of rows: ${moviesDF.count()}")

  }
}