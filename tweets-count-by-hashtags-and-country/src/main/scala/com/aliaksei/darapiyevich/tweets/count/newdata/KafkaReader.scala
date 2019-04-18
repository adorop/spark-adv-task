package com.aliaksei.darapiyevich.tweets.count.newdata

import com.aliaksei.darapiyevich.tweets.count.configuration.{KafkaReaderConfiguration, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaReader extends ((SparkSession, KafkaReaderConfiguration) => DataFrame) {
  override def apply(spark: SparkSession, configuration: KafkaReaderConfiguration): DataFrame = {
    spark.read
      .format(KAFKA_FORMAT)
      .options(configuration)
      .load()
  }
}
