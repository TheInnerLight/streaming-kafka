package org.novelfs.streaming.kafka

import java.util
import java.util.concurrent.{Future, TimeUnit}

import cats.effect.IO
import cats.effect.concurrent.MVar
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.novelfs.streaming.kafka.producer.{KafkaProducerSubscription, ProducerRecord}
import org.apache.kafka.clients.producer.{Callback, RecordMetadata, Producer => ApacheKafkaProducer, ProducerRecord => ApacheProducerRecord}
import org.apache.kafka.common
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo}
import org.novelfs.streaming.kafka.effects.MonadKafkaProducer
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.novelfs.streaming.kafka.effects.io._
import org.novelfs.streaming.kafka.KafkaSdkConversions._

class MonadKafkaProducerLiftIOInstanceSpec extends FlatSpec with Matchers with MockFactory with GeneratorDrivenPropertyChecks with DomainArbitraries {

  implicit val ioContextShift = IO.contextShift(scala.concurrent.ExecutionContext.global)
  implicit val ioConcurrentEffect = IO.ioConcurrentEffect

  trait MonadKafkaConsumerLiftIOInstanceSpecContext {

    var recordMvar = MVar.empty[IO, ApacheProducerRecord[String, String]].unsafeRunSync()

    val mockProducer = new ApacheKafkaProducer[String, String]() {
      override def sendOffsetsToTransaction(offsets: util.Map[common.TopicPartition, OffsetAndMetadata], consumerGroupId: String): Unit = ???
      override def initTransactions(): Unit = ???
      override def beginTransaction(): Unit = ???
      override def flush(): Unit = ???
      override def commitTransaction(): Unit = ???
      override def partitionsFor(topic: String): util.List[PartitionInfo] = ???
      override def metrics(): util.Map[MetricName, _ <: Metric] = ???
      override def close(): Unit = ???
      override def close(timeout: Long, unit: TimeUnit): Unit = ???
      override def send(record: ApacheProducerRecord[String, String]): Future[RecordMetadata] = {
        recordMvar.put(record).unsafeRunSync()
        null
      }
      override def send(record: ApacheProducerRecord[String, String], callback: Callback): Future[RecordMetadata] = {
        recordMvar.put(record).unsafeRunSync()
        null
      }
      override def abortTransaction(): Unit = ???
    }

    val kafkaSubscription = KafkaProducerSubscription(mockProducer)
  }

  "send" should "call send with the supplied kafka record" in new MonadKafkaConsumerLiftIOInstanceSpecContext {
    forAll { producerRecord : ProducerRecord[String, String] =>
      MonadKafkaProducer[IO].send(producerRecord)(kafkaSubscription).unsafeRunSync()
      val actualApacheKafkaProducerRecord = recordMvar.take.unsafeRunSync()
      actualApacheKafkaProducerRecord shouldBe producerRecord.toKafkaSdk
    }
  }

}
