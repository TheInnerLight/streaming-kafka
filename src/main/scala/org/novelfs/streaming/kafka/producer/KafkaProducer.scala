package org.novelfs.streaming.kafka.producer

import cats.effect._
import cats.implicits._
import fs2._
import org.apache.kafka.clients.producer.{KafkaProducer => ConcreteApacheKafkaProducer, Producer => ApacheKafkaProducer}
import org.apache.kafka.common.serialization.{ByteArraySerializer, Serializer}
import org.novelfs.streaming.kafka.KafkaSdkConversions
//import org.slf4j.LoggerFactory
import KafkaSdkConversions._

final case class KafkaProducer[K, V] private (kafkaProducer : ApacheKafkaProducer[K, V])

object KafkaProducer {
  //private val log = LoggerFactory.getLogger(KafkaProducer.getClass)

  /**
    * A pipe that serialises records to bytes using supplied key and value serialisers
    */
  def serializer[F[_] : Async, K, V](keySerializer: Serializer[K], valueSerializer : Serializer[V]) : Pipe[F, ProducerRecord[K, V], Either[Throwable, ProducerRecord[Array[Byte], Array[Byte]]]] =
    _.evalMap(record =>
      Async[F].delay {
        val key = keySerializer.serialize(record.topic, record.key)
        val value = valueSerializer.serialize(record.topic, record.value)
        record.copy(key = key, value = value)
      }.attempt
    )

  /**
    * An effect that sends a supplied producer record to the supplier kafka producer
    */
  def sendRecord[F[_] : Async, K, V](record: ProducerRecord[K, V])(producer: KafkaProducer[K, V]): F[Unit] =
    Async[F].delay {
      producer.kafkaProducer.send(record.toKafkaSdk)
      ()
    }

  /**
    * An effect to create a kafka producer using the supplied producer config
    */
  def createProducer[F[_] : Async, K, V](producerConfig : KafkaProducerConfig[K, V]): F[KafkaProducer[Array[Byte], Array[Byte]]] =
    Async[F].delay {
      val props = KafkaProducerConfig.generateProperties(producerConfig)
      val producer = new ConcreteApacheKafkaProducer[Array[Byte], Array[Byte]](props, new ByteArraySerializer(), new ByteArraySerializer())
      KafkaProducer(producer)
    }

  /**
    * An effect to cleanup a supplied kafka producer
    */
  def cleanupProducer[F[_] : Async, K, V](producer : KafkaProducer[K, V]): F[Unit] =
    Async[F].delay(producer.kafkaProducer.close())

  /**
    * Creates a Pipe that can be used to submit a stream of producer records to Kafka
    */
  def apply[F[_] : Async, K, V](producerConfig : KafkaProducerConfig[K, V]) : Pipe[F, ProducerRecord[K, V], Either[Throwable, Unit]] = s =>
    Stream.bracket(createProducer(producerConfig))(producer => {
      s.through(serializer(producerConfig.keySerializer, producerConfig.valueSerializer))
        .observe1 {
          case Right(r) => sendRecord(r)(producer)
          case Left(_)  => Async[F].unit
        }
        .map(_.map(_ => ()))
    }, cleanupProducer[F, Array[Byte], Array[Byte]])


}