package org.movielens

import org.apache.spark.sql.SparkSession

/**
 * Abstractly represents a single analytic task/question
 *
 * @param spark : SparkSession
 */
abstract class Question(spark: SparkSession) {
  def run(): Unit
}
