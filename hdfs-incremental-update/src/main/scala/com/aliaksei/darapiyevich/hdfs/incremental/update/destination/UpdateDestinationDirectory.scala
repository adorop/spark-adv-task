package com.aliaksei.darapiyevich.hdfs.incremental.update.destination

import com.aliaksei.darapiyevich.hdfs.incremental.update.TargetPath
import com.aliaksei.darapiyevich.hdfs.incremental.update.util.FileSystemFacade

class UpdateDestinationDirectory(
                                  $: FileSystemFacade,
                                  targetPath: TargetPath
                                ) {
  def withDataFrom(deltaDirectory: String): Unit = {
    val deltaPartitions = $ ls deltaDirectory
    deltaPartitions
      .foreach(replaceOutputPartitionDirectoryWithDeltasOne)
  }

  private def replaceOutputPartitionDirectoryWithDeltasOne(deltaPartitionDirectory: String): Unit = {
    val outputPartitionDirectory = targetPath.partitionOutputPath(deltaPartitionDirectory)
    $ rmdir outputPartitionDirectory
    $.mv(deltaPartitionDirectory, outputPartitionDirectory)
  }
}
