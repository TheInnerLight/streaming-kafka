package org.novelfs.streaming.kafka

import java.nio.ByteBuffer

import cats.effect._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FreeSpec, Matchers}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetCommitCallback, Consumer => ApacheKafkaConsumer}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.concurrent.duration._
import collection.JavaConverters._
import fs2._
import org.apache.kafka.common.serialization.Deserializer

class KafkaConsumerSpec extends FreeSpec with Matchers with MockFactory with GeneratorDrivenPropertyChecks with DomainArbitraries {

  val rawKafkaConsumer = mock[ApacheKafkaConsumer[String, String]]

  "cleanupConsumer should call consumer.close()" in {
    (rawKafkaConsumer.close _ : () => Unit) expects() once()
    KafkaConsumer.cleanupConsumer[IO, String, String](rawKafkaConsumer).unsafeRunSync()
  }

  "poll should call consumer.poll with supplied duration" in {
    forAll { (d: FiniteDuration) =>
      (rawKafkaConsumer.poll _) expects(d.toMillis) once()
      KafkaConsumer.pollKafka[IO, String, String](rawKafkaConsumer)(d).unsafeRunSync()
    }
  }

  "commit offset map should call consumer.commitAsync with the supplied OffsetMap" in {
    forAll { (offsetMap : Map[TopicPartition, OffsetMetadata]) =>
      val javaMap = offsetMap.map{case (tp, om) => TopicPartition.toKafkaTopicPartition(tp) -> OffsetMetadata.toKafkaOffsetMetadata(om) }.asJava

      (rawKafkaConsumer.commitAsync(_ : java.util.Map[org.apache.kafka.common.TopicPartition,  org.apache.kafka.clients.consumer.OffsetAndMetadata], _ : OffsetCommitCallback))
          .expects (javaMap, *) onCall { (_, callback) => callback.onComplete(javaMap, null) } once()

      KafkaConsumer.commitOffsetMap[IO, String, String](rawKafkaConsumer)(offsetMap).unsafeRunSync()
    }
  }

  "accumulate offset metadata should return the largest offsets for each topic/partition" in {
    forAll { consumerRecords: List[ConsumerRecord[String, String]] =>
      val finalMap = Stream.emits(consumerRecords)
          .through(KafkaConsumer.accumulateOffsetMetadata)
          .map(_._2)
          .toVector
          .last

      val expectedMap =
        consumerRecords.groupBy(cr => TopicPartition(cr.topic(), cr.partition()))
          .map{ case (k, v) => k -> v.map(_.offset()).max }

      finalMap === expectedMap
    }
  }

  "deserializer should call deserialize on the key and value deserializers with the supplied stream values" in {
    forAll { consumerRecords: List[ConsumerRecord[Array[Byte], Array[Byte]]] =>
      val intDeserializer = mock[Deserializer[Array[Int]]]

      (intDeserializer.deserialize (_ : String, _ : Array[Byte])) expects(*, *) onCall((_, arr) => TestHelpers.byteArrayToIntArray(arr)) atLeastOnce()

      val (ks: List[Array[Int]], vs : List[Array[Int]]) =
        Stream.emits(consumerRecords)
        .covary[IO]
        .through(KafkaConsumer.deserializer[IO, Array[Int], Array[Int]](intDeserializer, intDeserializer))
        .compile
        .toList
        .unsafeRunSync()
        .unzip

      val expectedKeys: List[Array[Int]] = consumerRecords.map(c => TestHelpers.byteArrayToIntArray(c.key)).toList
      val expectedVals: List[Array[Int]] = consumerRecords.map(c => TestHelpers.byteArrayToIntArray(c.value)).toList

      ks === expectedKeys
      vs === expectedVals
    }
  }


}
