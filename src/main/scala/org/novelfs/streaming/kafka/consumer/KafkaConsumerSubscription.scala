package org.novelfs.streaming.kafka.consumer

import cats.implicits._
import cats.effect.{Resource, Sync}
import org.apache.kafka.clients.consumer.{Consumer => ApacheKafkaConsumer, KafkaConsumer => ConcreteApacheKafkaConsumer}
import scala.collection.JavaConverters._

final case class KafkaConsumerSubscription[K, V] private (kafkaConsumer : ApacheKafkaConsumer[K, V])

object KafkaConsumerSubscription {
  def apply[F[_] : Sync, K, V](config : KafkaConsumerConfig[K, V]) : Resource[F, KafkaConsumerSubscription[K, V]] = {
    def create = {
      val consumer = new ConcreteApacheKafkaConsumer(KafkaConsumerConfig.generateProperties(config), config.keyDeserializer, config.valueDeserializer)
      Sync[F].delay (consumer.subscribe(config.topics.asJava)) *> Sync[F].pure(KafkaConsumerSubscription(consumer))
    }

    def destroy(subscription: KafkaConsumerSubscription[K, V]) = {
      Sync[F].delay(subscription.kafkaConsumer.wakeup()) *> Sync[F].delay(subscription.kafkaConsumer.close())
    }

    Resource.make(create)(destroy)
  }
}