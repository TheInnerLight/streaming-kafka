package org.novelfs.streaming.kafka

import cats.effect.IO
import org.novelfs.streaming.kafka.consumer.{KafkaConsumerSubscription, OffsetMetadata}
import org.novelfs.streaming.kafka.KafkaSdkConversions._
import org.apache.kafka.clients.consumer.{Consumer => ApacheKafkaConsumer, ConsumerRecord => ApacheConsumerRecord, ConsumerRecords => ApacheConsumerRecords}
import org.novelfs.streaming.kafka.interpreter.ThinKafkaConsumerClient
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._

class ThinKafkaConsumerClientSpec extends FlatSpec with Matchers with MockFactory with GeneratorDrivenPropertyChecks with DomainArbitraries {
  trait ThinKafkaConsumerClientSpecContext {
    val rawKafkaConsumer = mock[ApacheKafkaConsumer[String, String]]
    val kafkaSubscription = KafkaConsumerSubscription(rawKafkaConsumer)
  }

  "poll" should "call consumer.poll with supplied duration" in new ThinKafkaConsumerClientSpecContext {
    forAll { (d: FiniteDuration) =>
      (rawKafkaConsumer.poll _) expects(d.toMillis) returns
        (new ApacheConsumerRecords[String,String](Map.empty[org.apache.kafka.common.TopicPartition, java.util.List[ApacheConsumerRecord[String, String]]].asJava)) once()

      ThinKafkaConsumerClient[IO].poll(d)(kafkaSubscription).unsafeRunSync()
    }
  }

  "commit offset map" should "call consumer.commitSync with the supplied OffsetMap" in new ThinKafkaConsumerClientSpecContext {
    forAll { (offsetMap : Map[TopicPartition, OffsetMetadata]) =>
      val javaMap = offsetMap.toKafkaSdk

      (rawKafkaConsumer.commitSync(_ : java.util.Map[org.apache.kafka.common.TopicPartition,  org.apache.kafka.clients.consumer.OffsetAndMetadata]))
        .expects (javaMap) once()

      ThinKafkaConsumerClient[IO].commitOffsetMap(offsetMap)(kafkaSubscription).unsafeRunSync()
    }
  }

  "commit offset map" should "fail if an error was received from the callback" in new ThinKafkaConsumerClientSpecContext {
    forAll { (offsetMap : Map[TopicPartition, OffsetMetadata]) =>
      val javaMap = offsetMap.toKafkaSdk

      (rawKafkaConsumer.commitSync(_ : java.util.Map[org.apache.kafka.common.TopicPartition,  org.apache.kafka.clients.consumer.OffsetAndMetadata]))
        .expects (javaMap) onCall { _ => throw new RuntimeException("Argh") } once()

      val result = ThinKafkaConsumerClient[IO].commitOffsetMap(offsetMap)(kafkaSubscription).attempt.unsafeRunSync()

      result.isRight shouldBe false
    }
  }

  "topicPartitionAssignments" should "call consumer.assignment" in new ThinKafkaConsumerClientSpecContext {
    ((rawKafkaConsumer.assignment _) : () => java.util.Set[org.apache.kafka.common.TopicPartition]).expects().returns(new java.util.HashSet[org.apache.kafka.common.TopicPartition]())

    ThinKafkaConsumerClient[IO].topicPartitionAssignments(kafkaSubscription).unsafeRunSync()
  }
}
