package com.aliaksei.darapiyevich.tweets.count.configuration

case class PartitionConfiguration(
                                   partition: String,
                                   startIndex: Int,
                                   endIndex: Int
                                 ) {
}
