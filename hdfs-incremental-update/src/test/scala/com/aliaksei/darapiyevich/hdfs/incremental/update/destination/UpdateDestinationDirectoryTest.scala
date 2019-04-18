package com.aliaksei.darapiyevich.hdfs.incremental.update.destination

import com.aliaksei.darapiyevich.hdfs.incremental.update.TargetPath
import com.aliaksei.darapiyevich.hdfs.incremental.update.util.FileSystemFacade
import com.aliaksei.darapiyevich.sparkadv.spec.UnitTestSpec
import org.mockito.Mockito
import org.mockito.Mockito._

class UpdateDestinationDirectoryTest extends UnitTestSpec {

  private trait Setup {
    val deltaDirectory: String = "delta"

    val firstDeltaPartition: String = "firstDeltaPartition"
    val secondDeltaPartition: String = "secondDeltaPartition"
    val firstOutputPartition: String = "firstOutputPartition"
    val secondOutputPartition: String = "secondOutputPartition"

    val fileSystemFacade: FileSystemFacade = initFileSystemFacade()
    val targetPath: TargetPath = initTargetPath()

    val updateDestinationDirectory: UpdateDestinationDirectory = new UpdateDestinationDirectory(
      fileSystemFacade, targetPath
    )

    private def initFileSystemFacade(): FileSystemFacade = {
      val fs = mock[FileSystemFacade]
      when(fs.ls(deltaDirectory)).thenReturn(List(firstDeltaPartition, secondDeltaPartition))
      fs
    }

    private def initTargetPath(): TargetPath = {
      val targetPath = mock[TargetPath]
      when(targetPath.partitionOutputPath(firstDeltaPartition)).thenReturn(firstOutputPartition)
      when(targetPath.partitionOutputPath(secondDeltaPartition)).thenReturn(secondOutputPartition)
      targetPath
    }
  }

  "update" should "remove output partitions directories and move the ones from delta there" in new Setup {
    updateDestinationDirectory withDataFrom deltaDirectory
    verifyReplacesOutputPartitionsWithDeltaOnes()

    private def verifyReplacesOutputPartitionsWithDeltaOnes(): Unit = {
      val inOrder = Mockito.inOrder(fileSystemFacade)
      inOrder.verify(fileSystemFacade).rmdir(firstOutputPartition)
      inOrder.verify(fileSystemFacade).mv(firstDeltaPartition, firstOutputPartition)
      inOrder.verify(fileSystemFacade).rmdir(secondOutputPartition)
      inOrder.verify(fileSystemFacade).mv(secondDeltaPartition, secondOutputPartition)
    }
  }
}
