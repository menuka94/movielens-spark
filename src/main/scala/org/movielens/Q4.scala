package org.movielens

import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/**
 * Find the top-3 combinations of genres that have the highest ratings
 *
 * @param spark : SparkSession
 */
class Q4(spark: SparkSession) extends Question(spark) {
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
      // add new column 'numColumns' to represent the number of genres
      .withColumn("numGenres",
        functions.size(functions.split($"genres", "\\|")))
      // cast ratings to floats
      .withColumn("rating", $"rating".cast("float"))
      // cast numGenres to integers
      .withColumn("numGenres", $"numGenres".cast("integer"))
      // group genres by average rating
      .groupBy("genres")
      .mean("rating")
      // sort rows by average rating
      .orderBy(desc("avg(rating)"))


    println(genresRatingsDF.first())

    FileUtil.writeOutput("q4", genresRatingsDF, spark)
  }
}
