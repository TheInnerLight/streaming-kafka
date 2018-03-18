package org.novelfs.streaming.kafka

import org.novelfs.streaming.kafka.consumer.{ConsumerRecord, KafkaConsumerConfig, OffsetMetadata}
import org.novelfs.streaming.kafka.producer.ProducerRecord
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

trait DomainArbitraries {

  implicit val finiteDurationArb: Arbitrary[FiniteDuration] = Arbitrary(Gen.choose(0, 1000).map(_.milliseconds))

  implicit val offsetMap: Arbitrary[Map[TopicPartition, OffsetMetadata]] =
    Arbitrary(Gen.mapOf(for {
      topic <- Gen.alphaStr
      partition <- Arbitrary.arbInt.arbitrary
      offset <- Arbitrary.arbLong.arbitrary
    } yield (TopicPartition(topic, partition), OffsetMetadata(offset))))

  implicit val topicPartitionArb: Arbitrary[TopicPartition] =
    Arbitrary(for {
      topic <- Gen.nonEmptyListOf(Gen.alphaChar).map(_.toString)
      partition <- Arbitrary.arbInt.arbitrary
    } yield TopicPartition(topic, partition))

  implicit val topicPartitionListArb: Arbitrary[List[TopicPartition]] =
    Arbitrary(Gen.listOfN(5, topicPartitionArb.arbitrary))

  implicit val stringConsumerRecordsArb: Arbitrary[List[ConsumerRecord[String, String]]] =
    Arbitrary(for{
      topicPartitionList <- topicPartitionListArb.arbitrary
      tempRecords <- Gen.listOfN(1000, for {
        tp <- Gen.oneOf(topicPartitionList)
        key <- Gen.alphaStr
        value <- Gen.alphaStr
      } yield (tp, key, value))
      records <- tempRecords.zipWithIndex.map { case ((tp, key, value), i) =>
        ConsumerRecord[String, String](tp, i.toLong, key, value) }
    } yield records)

  implicit val byteArrayConsumerRecordsArb: Arbitrary[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
    Arbitrary(for{
      topicPartitionList <- topicPartitionListArb.arbitrary
      tempRecords <- Gen.listOfN(1000, for {
        tp <- Gen.oneOf(topicPartitionList)
        key <- Gen.listOfN(128, Arbitrary.arbByte.arbitrary)
        value <- Gen.listOfN(256, Arbitrary.arbByte.arbitrary)
      } yield (tp, key, value))
      records <- tempRecords.zipWithIndex.map { case ((tp, key, value), i) =>
        ConsumerRecord[Array[Byte], Array[Byte]](tp, i.toLong, key.toArray, value.toArray) }
    } yield records)

  implicit val stringProducerRecord : Arbitrary[ProducerRecord[String, String]] =
    Arbitrary(for {
      topic <- Gen.alphaStr
      key <- Gen.alphaStr
      value <- Gen.alphaStr
    } yield ProducerRecord(topic, None, key, value))

  implicit val kafkaEncryptionSettingsArb: Arbitrary[KafkaEncryptionSettings] =
    Arbitrary(for {
      location <- Gen.alphaStr
      password <- Gen.alphaStr
    } yield (KafkaEncryptionSettings(location, password)))

  implicit val kafkaAuthenticationSettingsArb: Arbitrary[KafkaAuthenticationSettings] =
    Arbitrary(for {
      location <- Gen.alphaStr
      storePassword <- Gen.alphaStr
      password <- Gen.alphaStr
    } yield (KafkaAuthenticationSettings(location, storePassword, password)))

  implicit val kafkaKafkaSecuritySettingsArb: Arbitrary[KafkaSecuritySettings] =
    Arbitrary(for {
      x <- Gen.choose(0, 3)
      securitySettings <- x match {
      case 0 => Gen.const(KafkaSecuritySettings.NoSecurity)
      case 1 => kafkaEncryptionSettingsArb.arbitrary.map(KafkaSecuritySettings.EncryptedNotAuthenticated)
      case 2 => kafkaAuthenticationSettingsArb.arbitrary.map(KafkaSecuritySettings.AuthenticatedNotEncrypted)
      case 3 => for {
          encSettings <- kafkaEncryptionSettingsArb.arbitrary
          authSettings <- kafkaAuthenticationSettingsArb.arbitrary
        } yield KafkaSecuritySettings.EncryptedAndAuthenticated(encSettings, authSettings)
      }
    } yield securitySettings)

  implicit val kafkaConsumerConfigArb: Arbitrary[KafkaConsumerConfig[String, String]] =
    Arbitrary(for {
      brokers <- Gen.listOfN(3, Gen.alphaStr)
      securitySettings <- kafkaKafkaSecuritySettingsArb.arbitrary
      topics <- Gen.listOfN(3, Gen.alphaStr)
      clientId <- Gen.alphaStr
      groupId <- Gen.alphaStr
    } yield KafkaConsumerConfig[String, String](brokers, securitySettings, topics, clientId, groupId, null, null))


}
