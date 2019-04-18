package com.aliaksei.darapiyevich.hdfs.incremental.update

import com.aliaksei.darapiyevich.hdfs.incremental.update.destination.UpdateDestinationDirectory
import com.aliaksei.darapiyevich.hdfs.incremental.update.merge.MergeOldAndNewData
import com.aliaksei.darapiyevich.hdfs.incremental.update.read.ReadOldData
import com.aliaksei.darapiyevich.hdfs.incremental.update.util.FileSystemFacade
import com.aliaksei.darapiyevich.hdfs.incremental.update.write.WriteToDelta
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class UpdateHdfsTableJob(
                          readOldData: () => DataFrame,
                          newData: DataFrame,
                          merge: (() => DataFrame, DataFrame) => DataFrame,
                          writeToDelta: DataFrame => String,
                          updateDestinationDirectory: UpdateDestinationDirectory
                        ) {

  def run(): Unit = {
    val merged = merge(readOldData, newData)
    val delta = writeToDelta(merged)
    updateDestinationDirectory withDataFrom delta
  }
}

class JobBuilder {
  private var spark: SparkSession = _
  private var schema: StructType = _
  private var readOptions: Map[String, String] = _
  private var outputFormat: String = _
  private var outputPath: String = _
  private var newData: DataFrame = _
  private var partitionColumn: String = _
  private var groupByColumns: List[String] = _
  private var aggregateColumn: String = _
  private var mergeAggregateValues: String => Column = _
  private var writeOptions: Map[String, String] = _

  def spark(sparkSession: SparkSession): JobBuilder = {
    spark = sparkSession
    this
  }

  def schema(schema: StructType): JobBuilder = {
    this.schema = schema
    this
  }

  def readOptions(options: Map[String, String]): JobBuilder = {
    this.readOptions = options
    this
  }

  def outputFormat(format: String): JobBuilder = {
    this.outputFormat = format
    this
  }

  def outputPath(outputPath: String): JobBuilder = {
    this.outputPath = outputPath
    this
  }

  def newData(newData: DataFrame): JobBuilder = {
    this.newData = newData
    this
  }

  def partitionColumn(partitionColumn: String): JobBuilder = {
    this.partitionColumn = partitionColumn
    this
  }

  def groupByColumns(groupByColumns: List[String]): JobBuilder = {
    this.groupByColumns = groupByColumns
    this
  }

  def aggregateColumn(aggregateColumn: String): JobBuilder = {
    this.aggregateColumn = aggregateColumn
    this
  }

  def mergeOldAndNewAggregateValue(expression: String => Column): JobBuilder = {
    this.mergeAggregateValues = expression
    this
  }

  def writeOptions(writeOptions: Map[String, String]): JobBuilder = {
    this.writeOptions = writeOptions
    this
  }

  def build(): UpdateHdfsTableJob = {
    val targetPath = new TargetPath()
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    new UpdateHdfsTableJob(
      new ReadOldData(spark, schema, readOptions, outputFormat, outputPath),
      newData,
      new MergeOldAndNewData(spark, partitionColumn, groupByColumns, aggregateColumn, mergeAggregateValues),
      new WriteToDelta(writeOptions, outputFormat, partitionColumn, targetPath, outputPath),
      new UpdateDestinationDirectory(new FileSystemFacade(fileSystem), targetPath)
    )
  }
}

object UpdateHdfsTableJob {
  def builder: JobBuilder = new JobBuilder
}

