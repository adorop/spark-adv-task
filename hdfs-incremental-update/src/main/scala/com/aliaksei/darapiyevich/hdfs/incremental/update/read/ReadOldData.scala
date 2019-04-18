package com.aliaksei.darapiyevich.hdfs.incremental.update.read

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadOldData(
                   spark: SparkSession,
                   schema: StructType,
                   readerOptions: Map[String, String],
                   format: String,
                   path: String
                 ) extends (() => DataFrame) {
  override def apply(): DataFrame = {
    spark.read
      .schema(schema)
      .format(format)
      .options(readerOptions)
      .load(path)
  }
}
