package com.aliaksei.darapiyevich.tweets.count.configuration

case class HdfsTableConfiguration(
                              readOptions: Map[String, String]=Map.empty,
                              format: String,
                              path: String,
                              writeOptions: Map[String, String]=Map.empty
                            ) {
}
