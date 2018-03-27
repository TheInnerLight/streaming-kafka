package org.novelfs.streaming.kafka.consumer

import org.novelfs.streaming.kafka.{KafkaHeader, TopicPartition}

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
final case class ConsumerRecord[K, V](
    topicPartition : TopicPartition,
    offset : Long,
    key : K,
    value : V,
    timestamp : Option[Long],
    serializedKeySize : Option[Int],
    serializedValueSize : Option[Int],
    headers : List[KafkaHeader]
    )

object ConsumerRecord {
  def apply[K, V](topicPartition: TopicPartition, offset: Long, key: K, value: V): ConsumerRecord[K, V] =
    new ConsumerRecord(topicPartition, offset, key, value, None, None, None, List.empty)
}
