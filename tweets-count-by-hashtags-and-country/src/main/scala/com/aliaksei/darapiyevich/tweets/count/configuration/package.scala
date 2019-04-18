package com.aliaksei.darapiyevich.tweets.count

import org.apache.spark.sql.types._

package object configuration {
  val KAFKA_BOOTSTRAP_SERVERS: String = "kafka.bootstrap.servers"
  val KAFKA_TOPIC: String = "subscribe"
  val KAFKA_TOPIC_STARTING_OFFSETS: String = "startingOffsets"
  val KAFKA_TOPIC_ENDING_OFFSETS: String = "endingOffsets"
  val KAFKA_FORMAT: String = "kafka"
  val KAFKA_MESSAGE_VALUE_COLUMN: String = "value"

  val HASH_TAG_COLUMN: String = "hashTag"
  val COUNTRY_COLUMN = "country"
  val PARTITION_COLUMN: String = "hour"
  val GROUP_BY_COLUMNS: List[String] = List(HASH_TAG_COLUMN, COUNTRY_COLUMN)
  val AGGREGATE_COLUMN: String = "count"

  val HDFS_TABLE_SCHEMA: StructType = new StructType()
    .add(StructField(PARTITION_COLUMN, StringType))
    .add(StructField(HASH_TAG_COLUMN, StringType))
    .add(StructField(COUNTRY_COLUMN, StringType))
    .add(StructField(AGGREGATE_COLUMN, LongType))

}
