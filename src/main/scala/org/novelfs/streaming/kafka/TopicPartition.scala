package org.novelfs.streaming.kafka

import org.apache.kafka.common

case class TopicPartition(topic : String, partition : Int)

object TopicPartition {
  def toKafkaTopicPartition(tp : TopicPartition): common.TopicPartition = {
    new org.apache.kafka.common.TopicPartition(tp.topic, tp.partition)
  }
}
