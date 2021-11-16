package org.movielens

import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * How many movies were released for every year within the dataset?
 * The title column of movies.csv includes the year each movie was published. Some movies might not
 * have the year, in such cases you can ignore those movies.
 */
class Q1(spark: SparkSession) extends Question(spark) {
  override def run(): Unit = {
    println("Question 1: ")

    val moviesDF: DataFrame = FileUtil.readCsv("movies.csv", spark)

    println(s"No. of rows: ${moviesDF.count()}")

    // regex to find year in movie title
    // i.e. extracts "(1995)" from "Toy Story (1995)"
    // should not try to extract 4 digits without the parentheses
    // as the movie tile could have a year i.e. 2012 (2009)
    val pattern = "\\(\\d{4}\\)"

    val yearsCount = moviesDF
      // extract year
      .withColumn("year", regexp_extract(moviesDF("title"), pattern, 0)
        // remove parentheses i.e. (1995) => 1995
        .substr(2, 4))
      // drop rows where year is N/A
      .na.drop(Seq("year"))
      // group year by number of rows
      .groupBy("year")
      .count()
      // sort by year
      .orderBy("year")

    println(yearsCount.show(yearsCount.count().toInt))

    FileUtil.writeOutput("q1", yearsCount, spark)
  }
}
