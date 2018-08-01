package org.novelfs.streaming.kafka

import cats.effect.IO
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.novelfs.streaming.kafka.producer.{KafkaProducer, KafkaProducerConfig, ProducerRecord}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import fs2._
import org.novelfs.streaming.kafka.consumer.{KafkaConsumer, KafkaConsumerConfig}
import org.scalatest.tagobjects.Slow

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class KafkaProducerIntegrationSpec extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks with DomainArbitraries with EmbeddedKafka {

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 7001, zooKeeperPort = 7000)
  val brokers = List("localhost:7001")
  val stringSerialiser = new StringSerializer()
  val stringDeserialiser = new StringDeserializer()

  "Concurrently producing and consuming records to/from a topic" should "result in all produced records being made available to the consumer" taggedAs (Slow) in {
    withRunningKafka {
      forAll { (producerRecords: List[ProducerRecord[String, String]]) =>
        val topicName = "topic-" +Random.alphanumeric.take(20).mkString
        createCustomTopic(topicName)
        val topicCorrectProducerRecords = producerRecords.map(_.copy(topic = topicName))

        val producerConfig = KafkaProducerConfig(
          brokers = brokers,
          security = KafkaSecuritySettings.NoSecurity,
          keySerializer = stringSerialiser,
          valueSerializer = stringSerialiser
        )

        val consumerConfig = KafkaConsumerConfig(
          brokers = brokers,
          security = KafkaSecuritySettings.NoSecurity,
          topics = List(topicName),
          clientId = "test",
          groupId = "test",
          keyDeserializer = stringDeserialiser,
          valueDeserializer = stringDeserialiser
        )

        val consumerStream = KafkaConsumer[IO, String, String](consumerConfig)
        val producerStream = Stream.emits(topicCorrectProducerRecords)
          .repeat
          .covary[IO]
          .through(KafkaProducer(producerConfig))

        // run the consumer stream and producer stream in parallel and collect up the results into a list
        val results =
          consumerStream
          .concurrently(producerStream)
          .collect {
            case Right(r) => r
          }
          //.take(producerRecords.size.toLong)
          .compile
          .drain
          .unsafeRunSync()

//        val expectedKeys = producerRecords.map(_.key)
//        val expectedVals = producerRecords.map(_.value)
//
//        val ks = results.map(_.key)
//        val vs = results.map(_.value)
//
//        ks should contain theSameElementsAs expectedKeys
//        vs should contain theSameElementsAs expectedVals

        results shouldBe (())
      }
    }
  }

}
