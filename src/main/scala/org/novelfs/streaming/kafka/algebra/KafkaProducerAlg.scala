package org.novelfs.streaming.kafka.algebra

import cats.Foldable
import org.novelfs.streaming.kafka.producer.ProducerRecord

trait KafkaProducerAlg[F[_], TContext[_, _]] {
  /**
    * An effect that sends a supplied producer record to the supplier kafka producer
    */
  def send[K, V](record : ProducerRecord[K, V])(context : TContext[K, V]) : F[Unit]

  /**
    * An effect that sends a supplied producer records to the supplier kafka producer
    */
  def send[K, V, G[_] : Foldable](records : G[ProducerRecord[K, V]])(context : TContext[K, V]) : F[Unit]
}
