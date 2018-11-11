package org.novelfs.streaming.kafka.producer

import cats.effect.{Resource, Sync}
import org.apache.kafka.clients.producer.{KafkaProducer => ConcreteApacheKafkaProducer, Producer => ApacheKafkaProducer}

final case class KafkaProducerSubscription[K, V] private (kafkaProducer : ApacheKafkaProducer[K, V])

object KafkaProducerSubscription {
  def apply[F[_] : Sync, K, V](config : KafkaProducerConfig[K, V]) : Resource[F, KafkaProducerSubscription[K, V]] = {
    def create = Sync[F].delay {
      val props = KafkaProducerConfig.generateProperties(config)
      val producer = new ConcreteApacheKafkaProducer[K, V](props, config.keySerializer, config.valueSerializer)
      KafkaProducerSubscription(producer)
    }

    def destroy(subscription: KafkaProducerSubscription[K, V]) = {
      Sync[F].delay(subscription.kafkaProducer.close())
    }

    Resource.make(create)(destroy)
  }
}
