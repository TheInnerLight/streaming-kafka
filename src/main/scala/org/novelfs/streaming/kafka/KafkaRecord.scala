package org.novelfs.streaming.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord

final case class KafkaHeader(key : String, value : Array[Byte])

final case class KafkaRecord[K, V] (
    topicPartition : TopicPartition,
    offset : Long,
    key : K,
    value : V,
    timestamp : Option[Long],
    serializedKeySize : Option[Int],
    serializedValueSize : Option[Int],
    headers : List[KafkaHeader]
    )

object KafkaRecord {
  def apply[K, V](topicPartition: TopicPartition, offset: Long, key: K, value: V): KafkaRecord[K, V] =
    new KafkaRecord(topicPartition, offset, key, value, None, None, None, List.empty)

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
