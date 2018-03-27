package org.novelfs.streaming.kafka

/**
  * @param key The key of the header
  * @param value The value of the header
  */
final case class KafkaHeader(key : String, value : Array[Byte])