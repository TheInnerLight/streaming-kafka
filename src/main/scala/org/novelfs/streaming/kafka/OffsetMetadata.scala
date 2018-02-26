package org.novelfs.streaming.kafka

case class OffsetMetadata(offset : Long)

object OffsetMetadata {
  def toKafkaOffsetMetadata(om : OffsetMetadata): org.apache.kafka.clients.consumer.OffsetAndMetadata = {
    new org.apache.kafka.clients.consumer.OffsetAndMetadata(om.offset)
  }
}
