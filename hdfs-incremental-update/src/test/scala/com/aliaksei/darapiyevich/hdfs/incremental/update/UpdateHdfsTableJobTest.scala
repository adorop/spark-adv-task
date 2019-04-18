package com.aliaksei.darapiyevich.hdfs.incremental.update

import com.aliaksei.darapiyevich.hdfs.incremental.update.destination.UpdateDestinationDirectory
import com.aliaksei.darapiyevich.sparkadv.spec.UnitTestSpec
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._

class UpdateHdfsTableJobTest extends UnitTestSpec {

  private trait Setup {
    val readOldData: () => DataFrame = mock[() => DataFrame]
    val newData: DataFrame = mock[DataFrame]
    val deltaDirectory = "/delta/directory"
    val mergedData: DataFrame = mock[DataFrame]
    val merge: (() => DataFrame, DataFrame) => DataFrame = initMerge()
    val writeToDelta: DataFrame => String = initDelta()
    val updateDestinationDirectory: UpdateDestinationDirectory = mock[UpdateDestinationDirectory]


    private def initMerge(): (() => DataFrame, DataFrame) => DataFrame = {
      val merge = mock[(() => DataFrame, DataFrame) => DataFrame]
      when(merge.apply(readOldData, newData)).thenReturn(mergedData)
      merge
    }

    private def initDelta(): DataFrame => String = {
      val writeToDelta = mock[DataFrame => String]
      when(writeToDelta.apply(mergedData)).thenReturn(deltaDirectory)
      writeToDelta
    }
  }

  "Job" should "update destination directory with merged old and new data from delta directory" in new Setup {
    new UpdateHdfsTableJob(readOldData, newData, merge, writeToDelta, updateDestinationDirectory)
      .run()
    verify(updateDestinationDirectory).withDataFrom(deltaDirectory)
  }
}
