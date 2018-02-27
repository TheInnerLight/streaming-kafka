package org.novelfs.streaming.kafka

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
}
