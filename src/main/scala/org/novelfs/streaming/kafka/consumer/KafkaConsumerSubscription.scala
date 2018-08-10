package org.novelfs.streaming.kafka.consumer

import cats.implicits._
import cats.effect.Sync
import org.apache.kafka.clients.consumer.{Consumer => ApacheKafkaConsumer, KafkaConsumer => ConcreteApacheKafkaConsumer}
import scala.collection.JavaConverters._

final case class KafkaConsumerSubscription[K, V] private (kafkaConsumer : ApacheKafkaConsumer[K, V])

object KafkaConsumerSubscription {

  /**
    * An effect that generates a subscription to some Kafka topics/paritions using the supplied kafka config
    */
  def apply[F[_] : Sync, K, V](config : KafkaConsumerConfig[K, V]): F[KafkaConsumerSubscription[K, V]] = {
    val consumer = new ConcreteApacheKafkaConsumer(KafkaConsumerConfig.generateProperties(config), config.keyDeserializer, config.valueDeserializer)
    Sync[F].delay (consumer.subscribe(config.topics.asJava)) *> Sync[F].pure(KafkaConsumerSubscription(consumer))
  }

  /**
    * An effect that disposes of some supplied subscription
    */
  def cleanup[F[_] : Sync, K, V](subscription : KafkaConsumerSubscription[K, V]): F[Unit] =
    Sync[F].delay(subscription.kafkaConsumer.wakeup()) *> Sync[F].delay(subscription.kafkaConsumer.close())

}