package org.movielens

import org.apache.spark.sql.functions.{asc, col, regexp_extract}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Q7(spark: SparkSession) extends Question(spark) {
  override def run(): Unit = {
    println("Question 7: ")

    val moviesDF: DataFrame = FileUtil.readCsv("movies.csv", spark)
    val ratingsDF: DataFrame = FileUtil.readCsv("ratings.csv", spark)

    val pattern = "[(]\\d{4}[)]$"

    val df = moviesDF.join(ratingsDF, "movieId")
      .withColumn("rating", col("rating").cast("Double"))
      .withColumn("year",
        regexp_extract(moviesDF("title"), pattern, 0)
          .substr(2, 4)
      )
      .filter("year != ''")

    val answer = df.groupBy("year")
      .avg("rating")
      .orderBy(asc("year"))

    answer.show(answer.count().toInt)

    FileUtil.writeOutput("q7", answer, spark)
  }
}
