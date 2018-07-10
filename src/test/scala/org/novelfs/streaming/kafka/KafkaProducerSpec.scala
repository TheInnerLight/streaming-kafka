package org.novelfs.streaming.kafka

import java.nio.charset.Charset

import cats.effect.IO
import org.novelfs.streaming.kafka.producer.{KafkaProducer, ProducerRecord}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.apache.kafka.clients.producer.{RecordMetadata, Producer => ApacheKafkaProducer}
import KafkaSdkConversions._
import fs2.Stream
import org.apache.kafka.clients
import org.apache.kafka.common.serialization.Serializer

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

  "serializer" should "call serialize on the key and value serializers with the supplied stream values" in {
    forAll { producerRecords: List[ProducerRecord[String, String]] =>
      val stringSerialiser = mock[Serializer[String]]

      (stringSerialiser.serialize (_ : String, _ : String)) expects(*, *) onCall((_, str) => str.getBytes(Charset.forName("UTF-8"))) atLeastOnce()

      val (ks, vs) =
        Stream.emits(producerRecords)
          .covary[IO]
          .through(KafkaProducer.serializer[IO, String, String](stringSerialiser, stringSerialiser))
          .collect { case Right(r) => r }
          .compile
          .toList
          .unsafeRunSync()
          .map(r => (r.key, r.value))
          .unzip

      val expectedKeys: List[Array[Byte]] = producerRecords.map(c => c.key.getBytes(Charset.forName("UTF-8")))
      val expectedVals: List[Array[Byte]] = producerRecords.map(c => c.value.getBytes(Charset.forName("UTF-8")))

      ks should contain theSameElementsAs expectedKeys
      vs should contain theSameElementsAs expectedVals
    }
  }

}
