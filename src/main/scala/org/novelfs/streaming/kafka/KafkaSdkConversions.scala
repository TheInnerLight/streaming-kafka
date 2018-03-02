package org.novelfs.streaming.kafka

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common

import scala.collection.JavaConverters._

trait FromKafkaSdk[A] {
  /**
    * Convert from a Kafka Sdk representation of the type into the strongly typed version from this library
    */
  def fromKafkaSdk : A
}

trait ToKafkaSdk[A] {
  /**
    * Convert from this library's representation of a type into the Kafka Sdk representation
    */
  def toKafkaSdk : A
}

trait FromSdkConversions {

  implicit class TopicPartitionFromKafkaSdk(tp : common.TopicPartition) extends FromKafkaSdk[TopicPartition] {
    override def fromKafkaSdk: TopicPartition = TopicPartition(tp.topic, tp.partition)
  }

  implicit class OffsetMetadataFromKafkaSdk(om : org.apache.kafka.clients.consumer.OffsetAndMetadata) extends FromKafkaSdk[OffsetMetadata] {
    override def fromKafkaSdk: OffsetMetadata = OffsetMetadata(om.offset)
  }

  implicit class ConsumerRecordFromKafkaSdk[K, V](consumerRecord: ConsumerRecord[K, V]) extends FromKafkaSdk[KafkaRecord[K, V]] {
    def fromKafkaSdk: KafkaRecord[K, V] =
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

  implicit class MapFromKafkaSdkConversions[K1, V1, K2, V2](m : util.Map[K1, V1])(implicit ev : K1 => FromKafkaSdk[K2], ev2 : V1 => FromKafkaSdk[V2]) extends FromKafkaSdk[Map[K2, V2]] {
    def fromKafkaSdk: Map[K2, V2] =
      m.asScala
        .map{case (o1, o2) => o1.fromKafkaSdk -> o2.fromKafkaSdk}
        .toMap
  }

  implicit class SetFromKafkaSdkConversions[T1, T2](m : util.Set[T1])(implicit ev : T1 => FromKafkaSdk[T2]) extends FromKafkaSdk[Set[T2]] {
    def fromKafkaSdk: Set[T2] =
      m.asScala
        .map(_.fromKafkaSdk)
        .toSet
  }

  implicit class ConsumerRecordsFromKafkaSdkConversions[K, V](consumerRecords : ConsumerRecords[K, V]) extends FromKafkaSdk[Vector[KafkaRecord[K, V]]] {
    def fromKafkaSdk: Vector[KafkaRecord[K, V]] =
      consumerRecords.asScala
        .map(_.fromKafkaSdk)
        .toVector
  }
}

trait ToSdkConversions {

  implicit class TopicPartitionToKafkaSdk(tp : TopicPartition) extends ToKafkaSdk[common.TopicPartition] {
    def toKafkaSdk: common.TopicPartition = new common.TopicPartition(tp.topic, tp.partition)
  }

  implicit class OffsetMetadataToKafkaSdk(om : OffsetMetadata) extends ToKafkaSdk[org.apache.kafka.clients.consumer.OffsetAndMetadata] {
    override def toKafkaSdk: OffsetAndMetadata = new org.apache.kafka.clients.consumer.OffsetAndMetadata(om.offset)
  }

  implicit class MapToKafkaSdkConversions[K1, V1, K2, V2](m : Map[K1, V1])(implicit ev : K1 => ToKafkaSdk[K2], ev2 : V1 => ToKafkaSdk[V2]) extends ToKafkaSdk[util.Map[K2, V2]] {
    def toKafkaSdk: util.Map[K2, V2] =
      m.map{case (o1, o2) => o1.toKafkaSdk -> o2.toKafkaSdk}
        .asJava
  }
}

object KafkaSdkConversions extends FromSdkConversions with ToSdkConversions {

}
