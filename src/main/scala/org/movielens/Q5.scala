package org.movielens

import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Find the number of movies that have been tagged as "Comedy"
 * (ignore case i.e. consider both "Comedy" and "comedy")
 *
 * @param spark : SparkSession
 */
class Q5(spark: SparkSession) extends Question(spark) {
  override def run(): Unit = {
    import spark.sqlContext.implicits._
    println("Question 5")

    val moviesDF: DataFrame = FileUtil.readCsv("movie.csv", spark)

    val numComedyMovies = moviesDF
      // filter movies with genre "comedy" (ignoring case)
      .filter(lower($"genres").contains("comedy"))
      .count()

    println(s"No. of Comedy movies: $numComedyMovies")
  }
}
