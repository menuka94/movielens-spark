package org.movielens

object Constants {
  final val SPARK_MASTER: String = System.getenv("SPARK_MASTER")
  final val DATA_DIR: String = System.getenv("DATA_DIR")
  final val USE_HDFS: Boolean = System.getenv("USE_HDFS").toBoolean
}
