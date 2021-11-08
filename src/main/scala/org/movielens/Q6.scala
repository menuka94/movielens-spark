package org.movielens

import org.apache.spark.sql.functions.{countDistinct, explode, lower}
import org.apache.spark.sql.{SparkSession, functions}

/**
 * Find the different genres within the dataset.
 * Find how many movies were released within different genres.
 * A movie may span multiple genres; in such cases, that movie should be counted
 * in all the genres
 * @param spark : SparkSession
 */
class Q6 (spark: SparkSession) extends Question (spark) {
  override def run(): Unit = {
    import spark.sqlContext.implicits._
    println("Question 6")

    var moviesDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(s"${Constants.DATA_DIR}/movie.csv")

    moviesDF = moviesDF.withColumn("genres",
      explode(functions.split($"genres", "\\|")))
      .withColumn("genres", $"genres".cast("string"))
      .withColumn("genres", lower($"genres"))

    val uniqueGenres = moviesDF.select(countDistinct("genres"))
    println(s"No. of distinct genres: ${uniqueGenres.first()}")

    val moviesGenresDF = moviesDF
      .groupBy("genres")
      .count()
      .orderBy("genres")

    moviesGenresDF.show(moviesGenresDF.count().toInt)
  }
}
