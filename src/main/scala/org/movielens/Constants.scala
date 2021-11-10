package org.movielens

object Constants {
  final val SPARK_MASTER: String = System.getenv("SPARK_MASTER")
  final val LOCAL_DATA_DIR: String = System.getenv("LOCAL_DATA_DIR")
  final val USE_HDFS: Boolean = System.getenv("USE_HDFS").toBoolean
  final val HDFS_DATA_DIR: String = System.getenv("HDFS_DATA_DIR")
  final val HDFS_OUTPUT_DIR: String = System.getenv("HDFS_OUTPUT_DIR")
}
