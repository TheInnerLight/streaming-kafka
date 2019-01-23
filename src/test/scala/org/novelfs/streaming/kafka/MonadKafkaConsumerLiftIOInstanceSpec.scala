package org.novelfs.streaming.kafka

import cats.effect.IO
import org.novelfs.streaming.kafka.consumer.{KafkaConsumerSubscription, OffsetMetadata}
import org.novelfs.streaming.kafka.KafkaSdkConversions._
import org.apache.kafka.clients.consumer.{Consumer => ApacheKafkaConsumer, ConsumerRecord => ApacheConsumerRecord, ConsumerRecords => ApacheConsumerRecords}
import org.novelfs.streaming.kafka.effects.MonadKafkaConsumer
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._
import org.novelfs.streaming.kafka.effects.io._

class MonadKafkaConsumerLiftIOInstanceSpec extends FlatSpec with Matchers with MockFactory with GeneratorDrivenPropertyChecks with DomainArbitraries {
  trait MonadKafkaConsumerLiftIOInstanceSpecContext {
    val rawKafkaConsumer = mock[ApacheKafkaConsumer[String, String]]
    val kafkaSubscription = KafkaConsumerSubscription(rawKafkaConsumer)
  }

  "poll" should "call consumer.poll with supplied duration" in new MonadKafkaConsumerLiftIOInstanceSpecContext {
    forAll { (d: FiniteDuration) =>
      (rawKafkaConsumer.poll(_ : java.time.Duration)) expects(java.time.Duration.ofMillis(d.toMillis)) returns
        (new ApacheConsumerRecords[String,String](Map.empty[org.apache.kafka.common.TopicPartition, java.util.List[ApacheConsumerRecord[String, String]]].asJava)) once()

      MonadKafkaConsumer[IO, KafkaConsumerSubscription].poll(d)(kafkaSubscription).unsafeRunSync()
    }
  }

  "commit offset map" should "call consumer.commitSync with the supplied OffsetMap" in new MonadKafkaConsumerLiftIOInstanceSpecContext {
    forAll { (offsetMap : Map[TopicPartition, OffsetMetadata]) =>
      val javaMap = offsetMap.toKafkaSdk

      (rawKafkaConsumer.commitSync(_ : java.util.Map[org.apache.kafka.common.TopicPartition,  org.apache.kafka.clients.consumer.OffsetAndMetadata]))
        .expects (javaMap) once()

      MonadKafkaConsumer[IO, KafkaConsumerSubscription].commitOffsetMap(offsetMap)(kafkaSubscription).unsafeRunSync()
    }
  }

  "commit offset map" should "fail if an error was received from the callback" in new MonadKafkaConsumerLiftIOInstanceSpecContext {
    forAll { (offsetMap : Map[TopicPartition, OffsetMetadata]) =>
      val javaMap = offsetMap.toKafkaSdk

      (rawKafkaConsumer.commitSync(_ : java.util.Map[org.apache.kafka.common.TopicPartition,  org.apache.kafka.clients.consumer.OffsetAndMetadata]))
        .expects (javaMap) onCall { (_ : java.util.Map[org.apache.kafka.common.TopicPartition,  org.apache.kafka.clients.consumer.OffsetAndMetadata]) => throw new RuntimeException("Argh") } once()

      val result = MonadKafkaConsumer[IO, KafkaConsumerSubscription].commitOffsetMap(offsetMap)(kafkaSubscription).attempt.unsafeRunSync()

      result.isRight shouldBe false
    }
  }

  "topicPartitionAssignments" should "call consumer.assignment" in new MonadKafkaConsumerLiftIOInstanceSpecContext {
    ((rawKafkaConsumer.assignment _) : () => java.util.Set[org.apache.kafka.common.TopicPartition]).expects().returns(new java.util.HashSet[org.apache.kafka.common.TopicPartition]())

    MonadKafkaConsumer[IO, KafkaConsumerSubscription].topicPartitionAssignments(kafkaSubscription).unsafeRunSync()
  }

  "seekToBeginning" should "call consumer.seekToBeginning" in new MonadKafkaConsumerLiftIOInstanceSpecContext {
    forAll { (partitionSet : Set[TopicPartition]) =>

      val javaSet = partitionSet.toKafkaSdk

      (rawKafkaConsumer.seekToBeginning(_ : java.util.Collection[org.apache.kafka.common.TopicPartition]))
        .expects (javaSet) once()


      MonadKafkaConsumer[IO, KafkaConsumerSubscription].seekToBeginning(partitionSet)(kafkaSubscription).unsafeRunSync()
    }
  }

  "seekToEnd" should "call consumer.seekToEnd" in new MonadKafkaConsumerLiftIOInstanceSpecContext {
    forAll { (partitionSet : Set[TopicPartition]) =>

      val javaSet = partitionSet.toKafkaSdk

      (rawKafkaConsumer.seekToEnd(_ : java.util.Collection[org.apache.kafka.common.TopicPartition]))
        .expects (javaSet) once()


      MonadKafkaConsumer[IO, KafkaConsumerSubscription].seekToEnd(partitionSet)(kafkaSubscription).unsafeRunSync()
    }
  }

  "seekTo" should "call consumer.seek" in new MonadKafkaConsumerLiftIOInstanceSpecContext {
    forAll { (offsetMap: Map[TopicPartition, OffsetMetadata]) =>
      offsetMap.foreach{ case (tp, om) =>
        (rawKafkaConsumer.seek(_, _)).expects(tp.toKafkaSdk, om.toKafkaSdk.offset) once()
      }

      MonadKafkaConsumer[IO, KafkaConsumerSubscription].seekTo(offsetMap)(kafkaSubscription).unsafeRunSync()
    }
  }
}
