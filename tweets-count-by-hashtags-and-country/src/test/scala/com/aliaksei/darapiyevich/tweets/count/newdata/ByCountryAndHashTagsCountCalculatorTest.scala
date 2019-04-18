package com.aliaksei.darapiyevich.tweets.count.newdata

import com.aliaksei.darapiyevich.sparkadv.spec.SparkIntegrationTestSpec
import com.aliaksei.darapiyevich.tweets.count.configuration._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class ByCountryAndHashTagsCountCalculatorTest extends SparkIntegrationTestSpec {
  private val partition = "partition"

  private val hashTagToAppearThreeTimes = "threeTimesHashTag"
  private val hashTagToAppearOnes = "oneTimeHashTag"

  private val country = "BY"

  private val byCountryAndHashTagsCountCalculator = new ByCountryAndHashTagsCountCalculator

  it("should group by a single hash tag regardless of case") {
    val result = byCountryAndHashTagsCountCalculator(input)
      .collect()
    result should have size 2
    assert(findRowWithCount(result)(3).getAs[String](HASH_TAG_COLUMN) equalsIgnoreCase hashTagToAppearThreeTimes)
    assert(findRowWithCount(result)(1).getAs[String](HASH_TAG_COLUMN) equalsIgnoreCase hashTagToAppearOnes)
  }

  private def input: DataFrame = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(partition, Array(hashTagToAppearThreeTimes.toUpperCase), country),
          Row(partition, Array(hashTagToAppearThreeTimes, hashTagToAppearOnes), country),
          Row(partition, Array(hashTagToAppearThreeTimes.toLowerCase), country)
        )
      ), inputSchema
    )
  }

  private def inputSchema: StructType = {
    new StructType()
      .add(StructField(PARTITION_COLUMN, StringType))
      .add(StructField(HASH_TAG_COLUMN, ArrayType(StringType)))
      .add(StructField(COUNTRY_COLUMN, StringType))
  }

  private def findRowWithCount(result: Seq[Row])(count: Long): Row = {
    result
      .find(row => row.getAs[Long](AGGREGATE_COLUMN) == count)
      .get
  }
}
