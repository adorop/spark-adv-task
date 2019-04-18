package com.aliaksei.darapiyevich.tweets.count.newdata

import com.aliaksei.darapiyevich.sparkadv.spec.UnitTestSpec
import com.aliaksei.darapiyevich.tweets.count.configuration.KafkaReaderConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito._

class NewDataReaderTest extends UnitTestSpec {

  private trait Setup {
    val kafkaReaderConfiguration: KafkaReaderConfiguration = KafkaReaderConfiguration(List.empty, "topic", List.empty)
    val spark: SparkSession = initSparkSession()
    private val kafkaData = mock[DataFrame]
    val readKafka: (SparkSession, KafkaReaderConfiguration) => DataFrame = initKafkaReader()
    private val requiredFieldsOnly = mock[DataFrame]
    val pruneFields: DataFrame => DataFrame = initProjectionPruning()
    val newAggregatedData: DataFrame = mock[DataFrame]
    val aggregate: DataFrame => DataFrame = initAggregation()

    val newDataReader: NewDataReader = new NewDataReader(spark, readKafka, pruneFields, aggregate)

    private def initSparkSession(): SparkSession = {
      mock[SparkSession]
    }

    private def initKafkaReader(): (SparkSession, KafkaReaderConfiguration) => DataFrame = {
      val kafkaReader = mock[(SparkSession, KafkaReaderConfiguration) => DataFrame]
      when(kafkaReader.apply(spark, kafkaReaderConfiguration)).thenReturn(kafkaData)
      kafkaReader
    }

    private def initProjectionPruning(): DataFrame => DataFrame = {
      val pruneFields = mock[Function[DataFrame, DataFrame]]
      when(pruneFields.apply(kafkaData)).thenReturn(requiredFieldsOnly)
      pruneFields
    }

    private def initAggregation(): DataFrame => DataFrame = {
      val countCalculator = mock[Function[DataFrame, DataFrame]]
      when(countCalculator.apply(requiredFieldsOnly)).thenReturn(newAggregatedData)
      countCalculator
    }
  }

  "read" should "return aggregated data read from kafka" in new Setup {
    private val result = newDataReader.read(kafkaReaderConfiguration)
    result shouldBe newAggregatedData
  }
}
