package org.novelfs.streaming.kafka

import cats.effect.IO
import org.novelfs.streaming.kafka.producer.{KafkaProducer, ProducerRecord}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.apache.kafka.clients.producer.{RecordMetadata, Producer => ApacheKafkaProducer}
import KafkaSdkConversions._
import org.apache.kafka.clients

class KafkaProducerSpec extends FlatSpec with Matchers with MockFactory with GeneratorDrivenPropertyChecks with DomainArbitraries  {

  trait StringApacheKafkaProducer extends ApacheKafkaProducer[String, String] {
    override def send(record: clients.producer.ProducerRecord[String, String]): java.util.concurrent.Future[RecordMetadata]
  }

  val rawKafkaProducer = mock[StringApacheKafkaProducer]
  val kafkaProducer = KafkaProducer(rawKafkaProducer)

  "cleanupProducer" should "call producer.close()" in {
    (rawKafkaProducer.close _ : () => Unit) expects() once()
    KafkaProducer.cleanupProducer[IO, String, String](kafkaProducer).unsafeRunSync()
  }

  "sendRecord" should "call producer.send()" in {
    forAll { record : ProducerRecord[String, String] =>
      val apacheRecord = record.toKafkaSdk
      (rawKafkaProducer.send : org.apache.kafka.clients.producer.ProducerRecord[String, String] => java.util.concurrent.Future[RecordMetadata]) expects(apacheRecord) once()

      KafkaProducer.sendRecord[IO, String, String](record)(kafkaProducer).unsafeRunSync()
    }
  }
}
