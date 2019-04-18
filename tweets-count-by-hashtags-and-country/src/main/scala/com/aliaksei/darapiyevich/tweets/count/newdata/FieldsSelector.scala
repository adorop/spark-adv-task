package com.aliaksei.darapiyevich.tweets.count.newdata

import com.aliaksei.darapiyevich.tweets.count.configuration._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}

class FieldsSelector extends Function[DataFrame, DataFrame] {
  private val allMandatoryPropertiesPresent = column(PARTITION_COLUMN).isNotNull &&
    column(COUNTRY_COLUMN).isNotNull &&
    column(HASH_TAG_COLUMN).isNotNull

  override def apply(dataFrame: DataFrame): DataFrame = {
    dataFrame.select(column(KAFKA_MESSAGE_VALUE_COLUMN).cast(to = StringType) as KAFKA_MESSAGE_VALUE_COLUMN)
      .select(
        dayAndHour as PARTITION_COLUMN,
        country as COUNTRY_COLUMN,
        hashTags as HASH_TAG_COLUMN
      ).where(allMandatoryPropertiesPresent)
  }

  private def dayAndHour: Column = {
    val millis = get_json_object(column(KAFKA_MESSAGE_VALUE_COLUMN), "$.createdAt")
    val dateTime = from_unixtime(millis / 1000)
    concat(year(dateTime),
      month(dateTime),
      dayofmonth(dateTime),
      hour(dateTime)
    )
  }

  private def country: Column = {
    get_json_object(column(KAFKA_MESSAGE_VALUE_COLUMN), "$.place.countryCode")
  }

  private def hashTags: Column = {
    val hashTagsAsString = get_json_object(column(KAFKA_MESSAGE_VALUE_COLUMN), "$.hashtagEntities[*].text")
    split(makeCommaSeparatedValuesFromJsonArray(hashTagsAsString), ",")
  }

  private def makeCommaSeparatedValuesFromJsonArray(jsonArray: Column): Column = {
    val jsonArraySigns = """\[|"|\]"""
    regexp_replace(jsonArray, jsonArraySigns, "")
  }
}
