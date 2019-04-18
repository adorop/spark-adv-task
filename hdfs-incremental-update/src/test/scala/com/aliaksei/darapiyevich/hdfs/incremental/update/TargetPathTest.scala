package com.aliaksei.darapiyevich.hdfs.incremental.update

import com.aliaksei.darapiyevich.sparkadv.spec.UnitTestSpec

class TargetPathTest extends UnitTestSpec{
  private val outputPath = "/output"
  private val targetPath = new TargetPath()

  "delta" should "return path to sub directory of given one" in {
    val result = targetPath delta outputPath
    result shouldBe outputPath + "/" + targetPath.deltaSubDirectoryName
  }

  "partitionOutputPath" should "return moved from delta to parent directory path" in {
    import targetPath._
    val result = targetPath.partitionOutputPath(s"/output/$deltaSubDirectoryName/part")
    result shouldBe "/output/part"
  }
}
