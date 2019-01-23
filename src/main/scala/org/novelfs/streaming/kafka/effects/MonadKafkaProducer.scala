package org.novelfs.streaming.kafka.effects

import cats.Foldable
import org.novelfs.streaming.kafka.producer.ProducerRecord
import simulacrum.typeclass

trait MonadKafkaProducer[F[_], TContext[_, _]] {
  /**
    * An effect that sends a supplied producer record to the supplier kafka producer
    */
  def send[K, V](record : ProducerRecord[K, V])(context : TContext[K, V]) : F[Unit]

  /**
    * An effect that sends a supplied producer records to the supplier kafka producer
    */
  def sendN[K, V, G[_] : Foldable](records : G[ProducerRecord[K, V]])(context : TContext[K, V]) : F[Unit]
}

object MonadKafkaProducer {
  def apply[F[_], TContext[_, _]](implicit monadKafkaProducer: MonadKafkaProducer[F, TContext]) = monadKafkaProducer
}
