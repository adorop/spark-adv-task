package com.aliaksei.darapiyevich.hdfs.incremental.update.util

import org.apache.hadoop.fs.{FileSystem, Path}

class FileSystemFacade(fileSystem: FileSystem) {
  val Recursive = true

  def ls(dir: String): List[String] = {
    fileSystem.listStatus(new Path(dir))
      .map(_.getPath.toString)
      .toList
  }

  def mv(src: String, dst: String): Unit = {
    fileSystem.rename(new Path(src), new Path(dst))
  }

  def rmdir(dir: String): Unit = {
    fileSystem.delete(new Path(dir), Recursive)
  }
}
