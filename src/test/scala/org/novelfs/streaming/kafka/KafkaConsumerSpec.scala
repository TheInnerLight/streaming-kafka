package org.novelfs.streaming.kafka

import cats.implicits._
import cats.effect._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.apache.kafka.clients.consumer.{ConsumerRecord => ApacheConsumerRecord, ConsumerRecords => ApacheConsumerRecords, OffsetCommitCallback, Consumer => ApacheKafkaConsumer}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.concurrent.duration._
import collection.JavaConverters._
import fs2._
import org.apache.kafka.common.serialization.Deserializer
import KafkaSdkConversions._
import org.novelfs.streaming.kafka.consumer.{KafkaConsumer, OffsetMetadata}
import scala.concurrent.ExecutionContext.Implicits.global

class KafkaConsumerSpec extends FlatSpec with Matchers with MockFactory with GeneratorDrivenPropertyChecks with DomainArbitraries {

  val rawKafkaConsumer = mock[ApacheKafkaConsumer[String, String]]
  val kafkaConsumer = KafkaConsumer(rawKafkaConsumer)

  "cleanupConsumer" should "call consumer.wakeup() and consumer.close()" in {
    (rawKafkaConsumer.wakeup _ : () => Unit) expects() once()
    (rawKafkaConsumer.close _ : () => Unit) expects() once()
    KafkaConsumer.cleanupConsumer[IO, String, String](kafkaConsumer).unsafeRunSync()
  }

  "poll" should "call consumer.poll with supplied duration" in {
    forAll { (d: FiniteDuration) =>
      (rawKafkaConsumer.poll _) expects(d.toMillis) returns
        (new ApacheConsumerRecords[String,String](Map.empty[org.apache.kafka.common.TopicPartition, java.util.List[ApacheConsumerRecord[String, String]]].asJava)) once()
      KafkaConsumer.pollKafka[IO, String, String](kafkaConsumer)(d).unsafeRunSync()
    }
  }

  "commit offset map" should "call consumer.commitAsync with the supplied OffsetMap" in {
    forAll { (offsetMap : Map[TopicPartition, OffsetMetadata]) =>
      val javaMap = offsetMap.toKafkaSdk

      (rawKafkaConsumer.commitAsync(_ : java.util.Map[org.apache.kafka.common.TopicPartition,  org.apache.kafka.clients.consumer.OffsetAndMetadata], _ : OffsetCommitCallback))
        .expects (javaMap, *) onCall { (_, callback) => callback.onComplete(javaMap, null) } once()

      val errorSignal = async.signalOf[IO, Boolean](false).unsafeRunSync()

      (KafkaConsumer.commitOffsetMap[IO, String, String](kafkaConsumer)(offsetMap)(errorSignal) *> IO{Thread.sleep(500)}).unsafeRunSync()
    }
  }

  "commit offset map" should "trigger the error signal if an error was received from the callback" in {
    forAll { (offsetMap : Map[TopicPartition, OffsetMetadata]) =>
      val javaMap = offsetMap.toKafkaSdk

      (rawKafkaConsumer.commitAsync(_ : java.util.Map[org.apache.kafka.common.TopicPartition,  org.apache.kafka.clients.consumer.OffsetAndMetadata], _ : OffsetCommitCallback))
        .expects (javaMap, *) onCall { (_, callback) => callback.onComplete(javaMap, new Exception("Red alert!")) } once()

      val errorSignal = async.signalOf[IO, Boolean](false).unsafeRunSync()

      KafkaConsumer.commitOffsetMap[IO, String, String](kafkaConsumer)(offsetMap)(errorSignal).unsafeRunSync()

      val signalTrue = errorSignal.discrete.dropWhile(!_).head.compile.toList.unsafeRunSync().head

      signalTrue shouldBe true
    }
  }

  "accumulate offset metadata" should "return the largest offsets for each topic/partition" in {
    forAll { consumerRecords: List[consumer.ConsumerRecord[String, String]] =>
      val finalMap: Map[TopicPartition, OffsetMetadata] = Stream.emits(consumerRecords)
          .through(KafkaConsumer.accumulateOffsetMetadata)
          .map{case (_, offsets) => offsets}
          .toVector
          .last

      val expectedMap: Map[TopicPartition, OffsetMetadata] =
        consumerRecords.groupBy(_.topicPartition)
          .map{ case (k, v) => k -> OffsetMetadata(v.map(_.offset).max) }

      finalMap shouldBe expectedMap
    }
  }

  "deserializer" should "call deserialize on the key and value deserializers with the supplied stream values" in {
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
