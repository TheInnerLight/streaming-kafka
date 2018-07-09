package org.novelfs.streaming.kafka

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord => ApacheConsumerRecord, ConsumerRecords => ApacheConsumerRecords}
import org.apache.kafka.clients.producer.{ProducerRecord => ApacheProducerRecord}
import org.apache.kafka.common
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.novelfs.streaming.kafka.consumer.{ConsumerRecord, OffsetMetadata}
import org.novelfs.streaming.kafka.producer.ProducerRecord
import scala.collection.JavaConverters._

trait FromKafkaSdk[In, Out] {
  /**
    * Convert from a Kafka Sdk representation of the type into the strongly typed version from this library
    */
  def fromKafkaSdk(input : In) : Out
}

trait ToKafkaSdk[In, Out] {
  /**
    * Convert from this library's representation of a type into the Kafka Sdk representation
    */
  def toKafkaSdk(input : In) : Out
}

trait FromSdkConversions {

  implicit val topicPartitionFromKafkaSdk = new FromKafkaSdk[common.TopicPartition, TopicPartition] {
    override def fromKafkaSdk(tp: common.TopicPartition): TopicPartition = TopicPartition(tp.topic, tp.partition)
  }

  implicit val offsetMetadataFromKafkaSdk = new FromKafkaSdk[org.apache.kafka.clients.consumer.OffsetAndMetadata, OffsetMetadata] {
    override def fromKafkaSdk(om : org.apache.kafka.clients.consumer.OffsetAndMetadata): OffsetMetadata = OffsetMetadata(om.offset)
  }

  implicit def consumerRecordFromKafkaSdk[K, V] = new FromKafkaSdk[ApacheConsumerRecord[K, V], ConsumerRecord[K, V]] {
    override def fromKafkaSdk(consumerRecord: ApacheConsumerRecord[K, V]): ConsumerRecord[K, V] =
      ConsumerRecord(
        topicPartition = TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
        offset = consumerRecord.offset(),
        key = consumerRecord.key(),
        value = consumerRecord.value(),
        timestamp = consumerRecord.timestamp match {
          case ApacheConsumerRecord.NO_TIMESTAMP => None
          case x => Some(x)
        },
        serializedKeySize = consumerRecord.serializedKeySize match {
          case ApacheConsumerRecord.NULL_SIZE => None
          case x => Some(x)
        },
        serializedValueSize = consumerRecord.serializedValueSize match {
          case ApacheConsumerRecord.NULL_SIZE => None
          case x => Some(x)
        },
        headers = consumerRecord.headers().toArray.map(h => KafkaHeader(h.key, h.value)).toList
      )
  }

  implicit def mapFromKafkaSdk[K1, V1, K2, V2](implicit ev : FromKafkaSdk[K1, K2], ev2 : FromKafkaSdk[V1, V2]) = new FromKafkaSdk[util.Map[K1, V1], Map[K2, V2]] {
    override def fromKafkaSdk(m: util.Map[K1, V1]): Map[K2, V2] =
      m.asScala
        .map{case (o1, o2) => ev.fromKafkaSdk(o1) -> ev2.fromKafkaSdk(o2)}
        .toMap
  }

  implicit def setFromKafkaSdk[T1, T2](implicit ev : FromKafkaSdk[T1, T2]) = new FromKafkaSdk[util.Set[T1], Set[T2]] {
    override def fromKafkaSdk(s: util.Set[T1]): Set[T2] =
      s.asScala
        .map(ev.fromKafkaSdk)
        .toSet
  }

  implicit def consumerRecordsFromKafkaSdk[K, V] = new FromKafkaSdk[ApacheConsumerRecords[K, V], Vector[ConsumerRecord[K, V]]] {
    override def fromKafkaSdk(consumerRecords: ApacheConsumerRecords[K, V]): Vector[ConsumerRecord[K, V]] = {
      val conv = implicitly[FromKafkaSdk[ApacheConsumerRecord[K, V], ConsumerRecord[K, V]]]
      consumerRecords.asScala
        .map(conv.fromKafkaSdk)
        .toVector
    }
  }
}

trait ToSdkConversions {

  implicit val topicPartitionToKafkaSdk = new ToKafkaSdk[TopicPartition, common.TopicPartition] {
    override def toKafkaSdk(tp: TopicPartition): common.TopicPartition = new common.TopicPartition(tp.topic, tp.partition)
  }

  implicit val offsetMetadataToKafkaSdk = new ToKafkaSdk[OffsetMetadata, org.apache.kafka.clients.consumer.OffsetAndMetadata] {
    override def toKafkaSdk(om: OffsetMetadata): org.apache.kafka.clients.consumer.OffsetAndMetadata = new org.apache.kafka.clients.consumer.OffsetAndMetadata(om.offset)
  }

  implicit def mapToKafkaSdk[K1, V1, K2, V2](implicit ev : ToKafkaSdk[K1, K2], ev2 : ToKafkaSdk[V1, V2]) = new ToKafkaSdk[Map[K1, V1], util.Map[K2, V2]] {
    override def toKafkaSdk(m: Map[K1, V1]): util.Map[K2, V2] =
      m.map{case (o1, o2) => ev.toKafkaSdk(o1) -> ev2.toKafkaSdk(o2)}
        .asJava
  }

  implicit def producerRecordToKafkaSdk[K, V] = new ToKafkaSdk[ProducerRecord[K, V], ApacheProducerRecord[K, V]] {
    override def toKafkaSdk(kafkaRecord: ProducerRecord[K, V]): ApacheProducerRecord[K, V] = {
      val headers: Array[Header] = kafkaRecord.headers.toArray.map(h => new RecordHeader(h.key, h.value) )
      new ApacheProducerRecord[K, V](
        kafkaRecord.topic,
        kafkaRecord.partition.map(new java.lang.Integer(_)).orNull,
        kafkaRecord.timestamp.map(new java.lang.Long(_)).orNull,
        kafkaRecord.key,
        kafkaRecord.value,
        new RecordHeaders(headers)
      )
    }
  }
}

object KafkaSdkConversions extends FromSdkConversions with ToSdkConversions {
  implicit class FromSdkConversionsOps[In, Out](val input : In) extends AnyVal {
    def fromKafkaSdk(implicit converter : FromKafkaSdk[In, Out]): Out = converter.fromKafkaSdk(input)
  }

  implicit class ToSdkConversionsOps[In, Out](val input : In) extends AnyVal {
    def toKafkaSdk(implicit converter : ToKafkaSdk[In, Out]): Out = converter.toKafkaSdk(input)
  }
}
