package com.aliaksei.darapiyevich.hdfs.incremental.update.merge

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class MergeOldAndNewData(
                          spark: SparkSession,
                          partitionColumn: String,
                          groupByColumns: List[String],
                          aggregateColumn: String,
                          mergeOldAndNewAggregateValue: String => Column
                        ) extends ((() => DataFrame, DataFrame) => DataFrame) {

  private val oldDataView = "OLD_DATA"
  private val newDataView = "NEW_DATA"

  private lazy val partitionAndGroupByColumns = column(partitionColumn) :: groupByColumns.map(column)

  override def apply(readOldData: () => DataFrame, newData: DataFrame): DataFrame = {
    newData.union(getOldPartitionsToUpdate(readOldData(), newData))
      .groupBy(partitionAndGroupByColumns: _*)
      .agg(mergeOldAndNewAggregateValue(aggregateColumn) as aggregateColumn)
  }

  private def getOldPartitionsToUpdate(oldData: DataFrame, newData: DataFrame): DataFrame = {
    oldData.createOrReplaceTempView(oldDataView)
    newData.createOrReplaceTempView(newDataView)
    spark.sql(
      s"""SELECT $partitionColumn, ${groupByColumns.mkString(",")}, $aggregateColumn
        FROM $oldDataView
        WHERE $partitionColumn IN (
          SELECT $partitionColumn FROM $newDataView
        )"""
    )
  }
}
