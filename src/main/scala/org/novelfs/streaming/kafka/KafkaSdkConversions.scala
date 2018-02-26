package org.novelfs.streaming.kafka

import java.util

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common

import scala.collection.JavaConverters._

object KafkaSdkConversions {

  def toKafkaOffsetMap(offsetMap : Map[TopicPartition, OffsetMetadata]): util.Map[common.TopicPartition, OffsetAndMetadata] =
    offsetMap.map{case (tp, om) => TopicPartition.toKafkaTopicPartition(tp) -> OffsetMetadata.toKafkaOffsetMetadata(om) }.asJava

  def fromKafkaOffsetMap(kafkaOffsetMap : util.Map[common.TopicPartition, OffsetAndMetadata]) : Map[TopicPartition, OffsetMetadata] =
    kafkaOffsetMap.asScala.map(x => TopicPartition(x._1.topic(), x._1.partition()) -> OffsetMetadata(x._2.offset()))
      .toMap
}
