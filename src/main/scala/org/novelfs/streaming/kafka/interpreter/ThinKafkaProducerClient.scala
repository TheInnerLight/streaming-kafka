package org.novelfs.streaming.kafka.interpreter

import cats.Foldable
import cats.implicits._
import cats.effect.Sync
import org.novelfs.streaming.kafka.KafkaSdkConversions._
import org.novelfs.streaming.kafka.algebra.KafkaProducerAlg
import org.novelfs.streaming.kafka.producer.{KafkaProducerSubscription, ProducerRecord}

object ThinKafkaProducerClient {
  /**
    * Create a KafkaProducerAlg which interprets instructions using a thin wrapper around the Apache Kafka Producer
    */
  def apply[F[_] : Sync] = new KafkaProducerAlg[F, KafkaProducerSubscription] {

    /**
      * An effect that sends a supplied producer record to the supplier kafka producer
      */
    override def send[K, V](record: ProducerRecord[K, V])(context: KafkaProducerSubscription[K, V]): F[Unit] =
      Sync[F].delay { context.kafkaProducer.send(record.toKafkaSdk) } *> Sync[F].unit

    /**
      * An effect that sends a supplied producer records to the supplier kafka producer
      */
    override def send[K, V, G[_] : Foldable](records: G[ProducerRecord[K, V]])(context: KafkaProducerSubscription[K, V]): F[Unit] =
      records.traverse_(record => send(record)(context))
  }
}
