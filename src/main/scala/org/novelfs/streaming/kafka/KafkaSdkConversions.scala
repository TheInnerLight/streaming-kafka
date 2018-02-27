package org.novelfs.streaming.kafka

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common

import scala.collection.JavaConverters._

object KafkaSdkConversions {

  def toKafkaOffsetMap(offsetMap : Map[TopicPartition, OffsetMetadata]): util.Map[common.TopicPartition, OffsetAndMetadata] =
    offsetMap.map{case (tp, om) => TopicPartition.toKafkaTopicPartition(tp) -> OffsetMetadata.toKafkaOffsetMetadata(om) }.asJava

  def fromKafkaOffsetMap(kafkaOffsetMap : util.Map[common.TopicPartition, OffsetAndMetadata]) : Map[TopicPartition, OffsetMetadata] =
    kafkaOffsetMap.asScala
      .map{case (topicPartition, offsetMetadata) => TopicPartition(topicPartition.topic(), topicPartition.partition()) -> OffsetMetadata(offsetMetadata.offset())}
      .toMap

  /**
    * Converts the kafka model of Consumer Records to strongly-typed Kafka records
    */
  def fromConsumerRecord[K, V](consumerRecord: ConsumerRecord[K, V]): KafkaRecord[K, V] = {
    KafkaRecord(
      topicPartition = TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
      offset = consumerRecord.offset(),
      key = consumerRecord.key(),
      value = consumerRecord.value(),
      timestamp = consumerRecord.timestamp match {
        case ConsumerRecord.NO_TIMESTAMP => None
        case x => Some(x)
      },
      serializedKeySize = consumerRecord.serializedKeySize match {
        case ConsumerRecord.NULL_SIZE => None
        case x => Some(x)
      },
      serializedValueSize = consumerRecord.serializedValueSize match {
        case ConsumerRecord.NULL_SIZE => None
        case x => Some(x)
      },
      headers = consumerRecord.headers().toArray.map(h => KafkaHeader(h.key, h.value)).toList
    )
  }
}
