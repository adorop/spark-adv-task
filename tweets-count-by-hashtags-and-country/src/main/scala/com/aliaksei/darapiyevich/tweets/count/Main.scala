package com.aliaksei.darapiyevich.tweets.count

import java.io.{FileInputStream, InputStreamReader}

import com.aliaksei.darapiyevich.hdfs.incremental.update.UpdateHdfsTableJob
import com.aliaksei.darapiyevich.tweets.count.configuration._
import com.aliaksei.darapiyevich.tweets.count.newdata.NewDataReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Path to configuration file is mandatory")
      System.exit(1)
    }
    val applicationConfiguration = readConfiguration(args(0))
    val job = buildJob(applicationConfiguration)
    job.run()
  }

  private def readConfiguration(path: String): ApplicationConfiguration = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    read[ApplicationConfiguration](new InputStreamReader(new FileInputStream(path)))
  }

  private def buildJob(applicationConfiguration: ApplicationConfiguration) = {
    val spark = SparkSession.builder()
      .getOrCreate()

    val hdfsTableConfiguration = applicationConfiguration.hdfsTableConfiguration

    UpdateHdfsTableJob.builder
      .spark(spark)
      .schema(HDFS_TABLE_SCHEMA)
      .readOptions(hdfsTableConfiguration.readOptions)
      .outputFormat(hdfsTableConfiguration.format)
      .outputPath(hdfsTableConfiguration.path)
      .newData(NewDataReader(spark).read(applicationConfiguration.kafkaReaderConfiguration))
      .partitionColumn(PARTITION_COLUMN)
      .groupByColumns(GROUP_BY_COLUMNS)
      .aggregateColumn(AGGREGATE_COLUMN)
      .mergeOldAndNewAggregateValue(sum)
      .writeOptions(hdfsTableConfiguration.writeOptions)
      .build()
  }
}
