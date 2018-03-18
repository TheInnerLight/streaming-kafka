package org.novelfs.streaming.kafka

import cats.effect.IO
import org.novelfs.streaming.kafka.producer.{KafkaProducer}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.apache.kafka.clients.producer.{Producer => ApacheKafkaProducer}

class KafkaProducerSpec extends FlatSpec with Matchers with MockFactory with GeneratorDrivenPropertyChecks with DomainArbitraries  {
  val rawKafkaProducer = mock[ApacheKafkaProducer[String, String]]
  val kafkaProducer = KafkaProducer(rawKafkaProducer)

  "cleanupProducer" should "call producer.close()" in {
    (rawKafkaProducer.close _ : () => Unit) expects() once()
    KafkaProducer.cleanupProducer[IO, String, String](kafkaProducer).unsafeRunSync()
  }


}
