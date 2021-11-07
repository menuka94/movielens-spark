package org.movielens

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_extract

import scala.collection.mutable

/**
 * How many movies were released for every year within the dataset?
 * The title column of movies.csv includes the year each movie was published. Some movies might not
 * have the year, in such cases you can ignore those movies.
 */
class Q1 (spark: SparkSession) extends Question (spark) {
  override def run(): Unit = {
    import spark.implicits._
    println("Question 1: ")

    val moviesDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(s"${Constants.DATA_DIR}/movie.csv")

    println(s"No. of rows: ${moviesDF.count()}")

    val pattern = "\\(\\d{4}\\)"

    val yearsCount = moviesDF.withColumn("year", regexp_extract(moviesDF("title"), pattern, 0)
      .substr(2, 4))
      .na.drop(Seq("year"))
      .groupBy("year")
      .count()
      .orderBy("year")

    println(yearsCount.show(10))
  }
}
