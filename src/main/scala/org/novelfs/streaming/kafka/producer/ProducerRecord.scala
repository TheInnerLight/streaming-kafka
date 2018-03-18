package org.novelfs.streaming.kafka.producer

import org.novelfs.streaming.kafka.{KafkaHeader}

/**
  * @param topic The topic this record will be sent to
  * @param partition The partition this record will be sent to (if none, the partition will be assigned by kafka)
  * @param key The key of the record
  * @param value The value of the record
  * @param timestamp The timestamp of the record (if none, the timestamp will be assigned by kafka)
  * @param headers The headers associated with the record
  */
final case class ProducerRecord[K, V](
   topic : String,
   partition : Option[Int],
   key : K,
   value : V,
   timestamp : Option[Long],
   headers : List[KafkaHeader]
  )

object ProducerRecord {
  def apply[K, V](topic : String, partition : Option[Int], key: K, value: V): ProducerRecord[K, V] =
    new ProducerRecord(topic, partition, key, value, None, List.empty)
}
