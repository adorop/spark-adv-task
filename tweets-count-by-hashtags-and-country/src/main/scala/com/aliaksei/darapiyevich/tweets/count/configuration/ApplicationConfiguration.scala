package com.aliaksei.darapiyevich.tweets.count.configuration

case class ApplicationConfiguration(
                                     kafkaReaderConfiguration: KafkaReaderConfiguration,
                                     hdfsTableConfiguration: HdfsTableConfiguration
                                   ) {

}
