package com.aliaksei.darapiyevich.hdfs.incremental.update

class TargetPath {
  private [update] val deltaSubDirectoryName = "_delta"

  def delta(outputPath: String): String = s"$outputPath/$deltaSubDirectoryName"

  def partitionOutputPath(delta: String): String = delta.replace(s"/$deltaSubDirectoryName", "")
}
