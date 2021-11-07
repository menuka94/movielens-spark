package org.movielens

import org.apache.spark.sql.SparkSession

/**
 * How many movies were released for every year within the dataset?
 * The title column of movies.csv includes the year each movie was published. Some movies might not
 * have the year, in such cases you can ignore those movies.
 */
class Q1 (spark: SparkSession) extends Question (spark) {
  override def run(): Unit = {
    println("Question 1: ")

    val moviesDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(s"${Constants.DATA_DIR}/movie.csv")

    println(s"No. of rows: ${moviesDF.count()}")

    val titles = moviesDF.select("title")
    println(s"No. of titles: ${titles.count()}")
  }
}
