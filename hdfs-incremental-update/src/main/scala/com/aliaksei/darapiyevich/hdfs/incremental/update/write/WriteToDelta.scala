package com.aliaksei.darapiyevich.hdfs.incremental.update.write

import com.aliaksei.darapiyevich.hdfs.incremental.update.TargetPath
import org.apache.spark.sql.{DataFrame, SaveMode}

class WriteToDelta(
                    writeOptions: Map[String, String],
                    writeFormat: String,
                    partitionColumn: String,
                    targetPath: TargetPath,
                    outputPath: String
                  ) extends (DataFrame => String) {
  override def apply(mergedData: DataFrame): String = {
    val delta = targetPath.delta(outputPath)
    mergedData.write
      .options(writeOptions)
      .format(writeFormat)
      .partitionBy(partitionColumn)
      .mode(SaveMode.Overwrite)
      .save(delta)
    delta
  }
}
