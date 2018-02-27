package org.novelfs.streaming.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * @param key The key of the header
  * @param value The value of the header
  */
final case class KafkaHeader(key : String, value : Array[Byte])

/**
  * @param topicPartition The topic and partition this record was received from
  * @param offset The offset of this record in the kafka partition
  * @param key The key of the record
  * @param value The value of the record
  * @param timestamp The timestamp of the record
  * @param serializedKeySize The length of the serialised key, if it exists
  * @param serializedValueSize The length of the serialised value, if it exists
  * @param headers The headers associated with the record
  */
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
