package org.novelfs.streaming.kafka

import cats.implicits._
import cats.effect._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.apache.kafka.clients.consumer.{Consumer => ApacheKafkaConsumer}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import fs2._
import org.apache.kafka.common.serialization.Deserializer
import org.novelfs.streaming.kafka.consumer.{KafkaConsumer, KafkaConsumerSubscription, OffsetMetadata}

import scala.concurrent.ExecutionContext.Implicits.global

class KafkaConsumerSpec extends FlatSpec with Matchers with MockFactory with GeneratorDrivenPropertyChecks with DomainArbitraries {

  trait KafkaConsumerSpecContext {
    val rawKafkaConsumer = mock[ApacheKafkaConsumer[String, String]]
    val kafkaSubscription = KafkaConsumerSubscription(rawKafkaConsumer)
  }

  "accumulate offset metadata" should "return the largest offsets for each topic/partition" in new KafkaConsumerSpecContext {
    forAll { consumerRecords: List[consumer.ConsumerRecord[String, String]] =>
      val finalMap: Map[TopicPartition, OffsetMetadata] = Stream.emits(consumerRecords)
          .covary[IO]
          .through(KafkaConsumer.accumulateOffsetMetadata)
          .map{case (_, offsets) => offsets}
          .compile
          .toVector
          .unsafeRunSync()
          .last

      val expectedMap: Map[TopicPartition, OffsetMetadata] =
        consumerRecords.groupBy(_.topicPartition)
          .map{ case (k, v) => k -> OffsetMetadata(v.map(_.offset).max) }

      finalMap shouldBe expectedMap
    }
  }

  "deserializer" should "call deserialize on the key and value deserializers with the supplied stream values" in new KafkaConsumerSpecContext {
    forAll { consumerRecords: List[consumer.ConsumerRecord[Array[Byte], Array[Byte]]] =>
      val intDeserializer = mock[Deserializer[Array[Int]]]

      (intDeserializer.deserialize (_ : String, _ : Array[Byte])) expects(*, *) onCall((_, arr) => TestHelpers.byteArrayToIntArray(arr)) atLeastOnce()

      val (ks, vs) =
        Stream.emits(consumerRecords)
          .covary[IO]
          .through(KafkaConsumer.deserializer[IO, Array[Int], Array[Int]](intDeserializer, intDeserializer))
          .collect { case Right(r) => r }
          .compile
          .toList
          .unsafeRunSync()
          .map(r => (r.key, r.value))
          .unzip

      val expectedKeys: List[Array[Int]] = consumerRecords.map(c => TestHelpers.byteArrayToIntArray(c.key))
      val expectedVals: List[Array[Int]] = consumerRecords.map(c => TestHelpers.byteArrayToIntArray(c.value))

      ks should contain theSameElementsAs expectedKeys
      vs should contain theSameElementsAs expectedVals
    }
  }


}
