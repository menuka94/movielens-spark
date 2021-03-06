package org.movielens

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

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

    val moviesDF: DataFrame = FileUtil.readCsv("movies.csv", spark)

    val ratingsDF: DataFrame = FileUtil.readCsv("ratings.csv", spark)

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
      // sort rows by average rating
      .orderBy("avg(rating)")

    genresRatingsDF.show(genresRatingsDF.count().toInt)

    FileUtil.writeOutput("q3", genresRatingsDF, spark)
  }
}
