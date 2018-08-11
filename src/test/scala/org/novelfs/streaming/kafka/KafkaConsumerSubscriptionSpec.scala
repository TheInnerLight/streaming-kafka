package org.novelfs.streaming.kafka

import cats.effect.IO
import org.novelfs.streaming.kafka.consumer.KafkaConsumerSubscription
import org.apache.kafka.clients.consumer.{Consumer => ApacheKafkaConsumer}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class KafkaConsumerSubscriptionSpec extends FlatSpec with Matchers with MockFactory with GeneratorDrivenPropertyChecks with DomainArbitraries  {

  trait KafkaConsumerSubscriptionSpecContext {
    val rawKafkaConsumer = mock[ApacheKafkaConsumer[String, String]]
    val kafkaSubscription = KafkaConsumerSubscription(rawKafkaConsumer)
  }

  "cleanupConsumer" should "call consumer.wakeup() and consumer.close()" in new KafkaConsumerSubscriptionSpecContext {
    (rawKafkaConsumer.wakeup _ : () => Unit) expects() once()
    (rawKafkaConsumer.close _ : () => Unit) expects() once()

    KafkaConsumerSubscription.cleanup[IO, String, String](kafkaSubscription).unsafeRunSync()
  }

}
