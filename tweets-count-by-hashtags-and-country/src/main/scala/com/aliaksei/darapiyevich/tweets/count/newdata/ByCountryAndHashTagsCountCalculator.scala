package com.aliaksei.darapiyevich.tweets.count.newdata

import com.aliaksei.darapiyevich.tweets.count.configuration._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class ByCountryAndHashTagsCountCalculator extends Function[DataFrame, DataFrame] {

  override def apply(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(
        column(PARTITION_COLUMN) as PARTITION_COLUMN,
        explode(column(HASH_TAG_COLUMN)) as HASH_TAG_COLUMN,
        column(COUNTRY_COLUMN) as COUNTRY_COLUMN
      )
      .groupBy(column(PARTITION_COLUMN), lower(column(HASH_TAG_COLUMN)) as HASH_TAG_COLUMN, column(COUNTRY_COLUMN))
      .agg(count("*") as AGGREGATE_COLUMN)
  }
}
