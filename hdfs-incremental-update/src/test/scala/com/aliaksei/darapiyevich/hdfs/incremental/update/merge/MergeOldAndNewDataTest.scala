package com.aliaksei.darapiyevich.hdfs.incremental.update.merge

import com.aliaksei.darapiyevich.sparkadv.spec.SparkIntegrationTestSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterAll

class MergeOldAndNewDataTest extends SparkIntegrationTestSpec with BeforeAndAfterAll {
  private val partitionColumn = "partition"
  private val groupByColumns = List("firstGroupBy", "secondGroupBy")
  private val aggregateColumn = "aggregate"

  private val partitionToBeUpdated = "new+old part"
  private val oldPartitionNotToBeIncluded = "old part"
  private val newPartition = "new part"

  private val groupToBeUpdated = ("firstGroup", 0)
  private val valueToBeUpdated = 333L
  private val oldGroupToBeUnchanged = ("firstGroup", 1)
  private val oldUnchangedValue = 22L
  private val newPartitionValue = 33L
  private val newGroup = ("newGroup", 0)
  private val newGroupValue = 44L

  private val valueToUpdateGroup = 5L

  private val merge = new MergeOldAndNewData(
    spark,
    partitionColumn,
    groupByColumns,
    aggregateColumn,
    mergeOldAndNewAggregateValue = sum)

  private var result: Array[Row] = _


  override protected def beforeAll(): Unit = {
    result = merge(oldData, newData)
      .collect()
  }

  private def oldData(): DataFrame = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(partitionToBeUpdated, groupToBeUpdated._1, groupToBeUpdated._2, valueToBeUpdated),
          Row(oldPartitionNotToBeIncluded, groupToBeUpdated._1, groupToBeUpdated._2, 1L),
          Row(partitionToBeUpdated, oldGroupToBeUnchanged._1, oldGroupToBeUnchanged._2, oldUnchangedValue)
        )
      ), schema
    )
  }

  private def schema: StructType = {
    new StructType()
      .add(StructField(partitionColumn, StringType))
      .add(StructField(groupByColumns.head, StringType))
      .add(StructField(groupByColumns(1), IntegerType))
      .add(StructField(aggregateColumn, LongType))
  }

  private def newData: DataFrame = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(newPartition, groupToBeUpdated._1, groupToBeUpdated._2, newPartitionValue),
          Row(partitionToBeUpdated, newGroup._1, newGroup._2, newGroupValue),
          Row(partitionToBeUpdated, groupToBeUpdated._1, groupToBeUpdated._2, valueToUpdateGroup)
        )
      ), schema
    )
  }

  it("should not include partitions which don't appear in new data") {
    val partitions = result
      .map(_.getString(0))
    partitions shouldNot contain(oldPartitionNotToBeIncluded)
  }

  it("should update value of existing partition when new data contains intersected row") {
    val rowExpectedToBeUpdated = result
      .find(row => byPartitionAndGroup(partitionToBeUpdated, groupToBeUpdated)(row))
    rowExpectedToBeUpdated shouldBe defined
    getAggregateValue(rowExpectedToBeUpdated) shouldBe valueToBeUpdated + valueToUpdateGroup
  }

  private def getAggregateValue(rowExpectedToBeUpdated: Option[Row]): Long = {
    rowExpectedToBeUpdated.get.getLong(3)
  }

  private def byPartitionAndGroup(partition: String, group: (String, Int))(row: Row): Boolean = {
    row.getString(0).equals(partition) && row.getString(1).equals(group._1) && row.getInt(2).equals(group._2)
  }

  it("should include groups from updated partitions which were not changed") {
    val rowExpectedToBeUnchanged = result
      .find(row => byPartitionAndGroup(partitionToBeUpdated, oldGroupToBeUnchanged)(row))
    rowExpectedToBeUnchanged shouldBe defined
    getAggregateValue(rowExpectedToBeUnchanged) shouldBe oldUnchangedValue
  }

  it("should include new groups from new data and old partition") {
    val rowExpectedToBeUnchanged = result
      .find(row => byPartitionAndGroup(partitionToBeUpdated, newGroup)(row))
    rowExpectedToBeUnchanged shouldBe defined
    getAggregateValue(rowExpectedToBeUnchanged) shouldBe newGroupValue
  }

  it("should include new partitions") {
    val rowExpectedToBeUnchanged = result
      .find(row => byPartitionAndGroup(newPartition, groupToBeUpdated)(row))
    rowExpectedToBeUnchanged shouldBe defined
    getAggregateValue(rowExpectedToBeUnchanged) shouldBe newPartitionValue
  }
}
