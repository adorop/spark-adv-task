package com.aliaksei.darapiyevich.tweets.count.configuration

import com.aliaksei.darapiyevich.sparkadv.spec.UnitTestSpec

class KafkaReaderConfigurationTest extends UnitTestSpec {
  private val firstServer = "firstServer"
  private val secondServer = "secondServer"
  private val topic = "topic"

  private val firstPartition = "firstPartition"
  private val secondPartition = "secondPartition"
  private val firstStartingOffset = 11
  private val secondStartingOffset = 10

  private val firstEndingOffset = 13
  private val secondEndingOffset = 12

  private val kafkaReaderConfiguration = KafkaReaderConfiguration(
    List(firstServer, secondServer),
    topic,
    List(
      PartitionConfiguration(firstPartition, firstStartingOffset, firstEndingOffset),
      PartitionConfiguration(secondPartition, secondStartingOffset, secondEndingOffset)
    )
  )

  "bootstrap servers" should "be comma concatenated" in new {
    kafkaReaderConfiguration(KAFKA_BOOTSTRAP_SERVERS) shouldBe s"$firstServer,$secondServer"
  }

  "starting offsets" should "be a json with properties partition to starting offset" in new {
    private val expectedStartingOffset = s"""{"$topic":{"$firstPartition":$firstStartingOffset,"$secondPartition":$secondStartingOffset}}"""
    kafkaReaderConfiguration(KAFKA_TOPIC_STARTING_OFFSETS) shouldBe expectedStartingOffset
  }

  "ending offsets" should "be a json with properties partition to ending offset" in new {
    private val expectedEndingOffsets = s"""{"$topic":{"$firstPartition":$firstEndingOffset,"$secondPartition":$secondEndingOffset}}"""
    kafkaReaderConfiguration(KAFKA_TOPIC_ENDING_OFFSETS) shouldBe expectedEndingOffsets
  }
}
