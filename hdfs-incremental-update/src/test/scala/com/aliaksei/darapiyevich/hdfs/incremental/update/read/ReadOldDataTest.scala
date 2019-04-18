package com.aliaksei.darapiyevich.hdfs.incremental.update.read

import com.aliaksei.darapiyevich.sparkadv.spec.UnitTestSpec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.mockito.Mockito._

class ReadOldDataTest extends UnitTestSpec {

  private trait Setup {
    val options: Map[String, String] = Map("arbitrary" -> "option")
    val format: String = "format"
    val path: String = "path"
    val schema: StructType = new StructType()

    val dataFrame: DataFrame = mock[DataFrame]
    val dataFrameReader: DataFrameReader = initDataFrameReader()
    val spark: SparkSession = initSpark()

    val readOldData: ReadOldData = new ReadOldData(spark, schema, options, format, path)

    private def initSpark(): SparkSession = {
      val spark = mock[SparkSession]
      when(spark.read).thenReturn(dataFrameReader)
      spark
    }

    private def initDataFrameReader(): DataFrameReader = {
      val dataFrameReader = mock[DataFrameReader]
      when(dataFrameReader.options(options)).thenReturn(dataFrameReader)
      when(dataFrameReader.format(format)).thenReturn(dataFrameReader)
      when(dataFrameReader.load(path)).thenReturn(dataFrame)
      when(dataFrameReader.schema(schema)).thenReturn(dataFrameReader)
      dataFrameReader
    }
  }

  "read" should "return data frame" in new Setup {
    private val result = readOldData()
    result shouldBe dataFrame
  }

  "read" should "configure reader's options" in new Setup {
    readOldData()
    verify(dataFrameReader).options(options)
  }

  "read" should "configure read format" in new Setup {
    readOldData()
    verify(dataFrameReader).format(format)
  }

  "read" should "configure read schema" in new Setup {
    readOldData()
    verify(dataFrameReader).schema(schema)
  }
}
