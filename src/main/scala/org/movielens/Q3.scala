package org.movielens

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.explode

/**
 * Rank the genres in the order of their ratings? Again, a movie may span multiple genres;
 * such a movie should be counted in all the genres.
 *
 * @param spark : SparkSession
 */
class Q3(spark: SparkSession) extends Question(spark) {
  override def run(): Unit = {
    import spark.sqlContext.implicits._
    println("Question 3")

    val moviesDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(s"${Constants.DATA_DIR}/movie.csv")

    val ratingsDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(s"${Constants.DATA_DIR}/rating.csv")

    val genresRatingsDF = moviesDF
      // join movies and ratings using movieId column
      .join(ratingsDF, "movieId")
      // select the necessary columns
      .select("genres", "rating")
      // split genres into multiple rows
      .withColumn("genres", explode(functions.split($"genres", "\\|")))
      // cast ratings to floats - required for calculating the average
      .withColumn("rating", $"rating".cast("float").alias("rating"))
      // group genres by average rating
      .groupBy("genres")
      .mean("rating")
      .orderBy("avg(rating)")

    genresRatingsDF.show(10)
  }
}
