package com.aliaksei.darapiyevich.tweets.count.configuration

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

case class KafkaReaderConfiguration(
                                     bootstrapServers: List[String],
                                     topic: String,
                                     partitions: List[PartitionConfiguration]
                                   ) {
}

object KafkaReaderConfiguration {

  implicit def config2map(kafkaReaderConfiguration: KafkaReaderConfiguration): Map[String, String] = {
    Map(
      KAFKA_BOOTSTRAP_SERVERS -> kafkaReaderConfiguration.bootstrapServers.mkString(","),
      KAFKA_TOPIC -> kafkaReaderConfiguration.topic,
      KAFKA_TOPIC_STARTING_OFFSETS -> getStartingOffsets(kafkaReaderConfiguration),
      KAFKA_TOPIC_ENDING_OFFSETS -> getEndingOffsets(kafkaReaderConfiguration)
    )
  }

  private def getStartingOffsets(kafkaReaderConfiguration: KafkaReaderConfiguration): String = {
    getOffsets(kafkaReaderConfiguration, partition => partition.startIndex)
  }

  private def getOffsets(kafkaReaderConfiguration: KafkaReaderConfiguration, offset: PartitionConfiguration => Int): String = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    val offsets: Map[String, Int] = kafkaReaderConfiguration.partitions
      .map(partitionConfiguration => partitionConfiguration.partition -> offset(partitionConfiguration))
      .toMap
    val option = kafkaReaderConfiguration.topic -> offsets
    compact(render(option))
  }

  private def getEndingOffsets(kafkaReaderConfiguration: KafkaReaderConfiguration): String = {
    getOffsets(kafkaReaderConfiguration, partition => partition.endIndex)
  }
}
