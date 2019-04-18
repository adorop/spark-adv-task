package com.aliaksei.darapiyevich.tweets.count.newdata

import com.aliaksei.darapiyevich.tweets.count.configuration.KafkaReaderConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}

class NewDataReader(
                     spark: SparkSession,
                     readKafka: (SparkSession, KafkaReaderConfiguration) => DataFrame,
                     pruneFields: DataFrame => DataFrame,
                     aggregate: DataFrame => DataFrame
                   ) {

  def read(kafkaReaderConfiguration: KafkaReaderConfiguration): DataFrame = {
    val kafkaData = readKafka(spark, kafkaReaderConfiguration)
    val requiredForAggregationFields = pruneFields(kafkaData)
    aggregate(requiredForAggregationFields)
  }
}

object NewDataReader {
  def apply(spark: SparkSession): NewDataReader = {
    new NewDataReader(spark, new KafkaReader, new FieldsSelector, new ByCountryAndHashTagsCountCalculator)
  }
}