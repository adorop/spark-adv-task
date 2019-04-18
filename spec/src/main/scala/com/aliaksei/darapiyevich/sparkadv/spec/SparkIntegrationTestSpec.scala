package com.aliaksei.darapiyevich.sparkadv.spec

import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Matchers}

trait SparkIntegrationTestSpec extends FunSpec with Matchers {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local")
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.shuffle.partitions", 1)
    .getOrCreate()
}
