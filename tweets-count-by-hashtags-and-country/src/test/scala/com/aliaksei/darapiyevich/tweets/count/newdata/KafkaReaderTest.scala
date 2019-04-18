package com.aliaksei.darapiyevich.tweets.count.newdata

import com.aliaksei.darapiyevich.sparkadv.spec.UnitTestSpec
import com.aliaksei.darapiyevich.tweets.count.configuration._
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.mockito.Mockito._

class KafkaReaderTest extends UnitTestSpec {

  private trait Setup {
    val configuration: KafkaReaderConfiguration = KafkaReaderConfiguration(List.empty, "topic", List.empty)

    val spark: SparkSession = mock[SparkSession]
    val kafkaData: DataFrame = mock[DataFrame]
    val dataFrameReader: DataFrameReader = initDataFrameReader()

    val kafkaReader: KafkaReader = new KafkaReader()

    private def initDataFrameReader(): DataFrameReader = {
      val reader = mock[DataFrameReader]
      when(spark.read).thenReturn(reader)
      when(reader.format(KAFKA_FORMAT)).thenReturn(reader)
      when(reader.options(configuration)).thenReturn(reader)
      when(reader.load()).thenReturn(kafkaData)
      reader
    }
  }

  "read" should "specify kafka format" in new Setup {
    kafkaReader(spark, configuration)
    verify(dataFrameReader).format(KAFKA_FORMAT)
  }

  "read" should "specify read options" in new Setup {
    kafkaReader(spark, configuration)
    verify(dataFrameReader).options(configuration)
  }

  "read" should "return dataFrame read from kafka" in new Setup {
    val result = kafkaReader(spark, configuration)
    result shouldBe kafkaData
  }
}
