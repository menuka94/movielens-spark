package org.movielens

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lower

/**
 * Find the number of movies that have been tagged as "Comedy"
 * (ignore case i.e. consider both "Comedy" and "comedy")
 * @param spark : SparkSession
 */
class Q5 (spark: SparkSession) extends Question (spark) {
  override def run(): Unit = {
    import spark.sqlContext.implicits._
    println("Question 5")

    val moviesDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(s"${Constants.DATA_DIR}/movie.csv")

    val numComedyMovies = moviesDF
      .filter(lower($"genres").contains("comedy"))
      .count()

    println(s"No. of Comedy movies: $numComedyMovies")
  }
}
