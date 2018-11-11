package org.novelfs.streaming.kafka.producer

import cats.effect._
import cats.implicits._
import fs2._
import org.apache.kafka.clients.producer.{KafkaProducer => ConcreteApacheKafkaProducer, Producer => ApacheKafkaProducer}
import org.apache.kafka.common.serialization.{ByteArraySerializer, Serializer}
import org.novelfs.streaming.kafka.KafkaSdkConversions
import org.novelfs.streaming.kafka.effects.MonadKafkaProducer
import KafkaSdkConversions._

final case class KafkaProducer[K, V] private (kafkaProducer : ApacheKafkaProducer[K, V])

object KafkaProducer {

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
    * Creates a Pipe that can be used to submit a stream of producer records to Kafka
    */
  def apply[F[_] : Async, K, V](producerConfig : KafkaProducerConfig[K, V])(implicit monadKafkaProducer: MonadKafkaProducer.Aux[F, KafkaProducerSubscription]) : Pipe[F, ProducerRecord[K, V], Either[Throwable, Unit]] = s =>
    Stream.resource(KafkaProducerSubscription(producerConfig.copy(keySerializer = new ByteArraySerializer(), valueSerializer = new ByteArraySerializer())))
      .flatMap(producer => {
        s.through(serializer(producerConfig.keySerializer, producerConfig.valueSerializer))
          .evalTap {
            case Right(r) => monadKafkaProducer.send(r)(producer)
            case Left(_)  => Async[F].unit
          }
          .map(_.map(_ => ()))
      })


}