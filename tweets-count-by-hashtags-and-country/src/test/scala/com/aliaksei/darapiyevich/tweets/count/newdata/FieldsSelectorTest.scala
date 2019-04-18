package com.aliaksei.darapiyevich.tweets.count.newdata

import java.net.URI
import java.nio.file.{Files, Paths}
import java.time.{Instant, LocalDateTime, ZoneId}

import com.aliaksei.darapiyevich.sparkadv.spec.SparkIntegrationTestSpec
import com.aliaksei.darapiyevich.tweets.count.configuration._
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s._
import org.scalatest.BeforeAndAfterAll

import scala.io.Source

class FieldsSelectorTest extends SparkIntegrationTestSpec with BeforeAndAfterAll {
  implicit val formats: DefaultFormats.type = DefaultFormats
  private val testTweet: URI = getClass.getResource("/tweet.json").toURI

  private val fieldsSelector = new FieldsSelector

  var expectedDayAndHour: String = _
  var expectedCountry: String = _
  var expectedHashTags: Seq[String] = _

  private var result: Row = _


  override protected def beforeAll(): Unit = {
    initExpected()
    result = fieldsSelector(input)
      .collect()(0)
  }


  private def initExpected(): Unit = {
    val tweet = readTestTweetResource()
    expectedDayAndHour = extractExpectedDayAndHour(tweet)
    expectedCountry = extractExpectedCountryCode(tweet)
    expectedHashTags = extractHashTagsTexts(tweet)
  }

  private def readTestTweetResource(): JValue = {
    import org.json4s.jackson.JsonMethods.parse

    parse(Source.fromFile(testTweet).mkString)
  }

  private def extractExpectedDayAndHour(tweet: JValue): String = {
    val millis: Long = (tweet \ "createdAt").extract[BigInt].longValue()
    val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault())
    s"${dateTime.getYear}${dateTime.getMonthValue}${dateTime.getDayOfMonth}${dateTime.getHour}"
  }

  private def extractExpectedCountryCode(tweet: JValue): String = {
    (tweet \ "place" \ "countryCode").extract[String]
  }

  private def extractHashTagsTexts(tweet: JValue): Seq[String] = {
    (tweet \ "hashtagEntities").extract[Seq[JValue]]
      .map(hashTagEntity => (hashTagEntity \ "text").extract[String])
  }

  private def input: DataFrame = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("key", tweet)
        )
      ), inputSchema
    )
  }

  private def tweet: Array[Byte] = {
    Files.readAllBytes(Paths.get(testTweet))
  }

  private def inputSchema: StructType = {
    new StructType()
      .add(StructField("key", StringType))
      .add(StructField(KAFKA_MESSAGE_VALUE_COLUMN, BinaryType))
  }


  it("partition should be date and hour concatenated") {
    result.getAs[String](PARTITION_COLUMN) shouldBe expectedDayAndHour
  }

  it("country should be equal to country code") {
    result.getAs[String](COUNTRY_COLUMN) shouldBe expectedCountry
  }

  it("hashTags should be equal to extracted hashTagEntities texts") {
    val hashTags = result.getAs[Seq[String]](HASH_TAG_COLUMN)
    hashTags shouldEqual expectedHashTags
  }
}
