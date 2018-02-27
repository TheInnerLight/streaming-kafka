package org.novelfs.streaming.kafka

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

  implicit val stringConsumerRecordsArb: Arbitrary[List[KafkaRecord[String, String]]] =
    Arbitrary(for{
      topicPartitionList <- topicPartitionListArb.arbitrary
      tempRecords <- Gen.listOfN(1000, for {
        tp <- Gen.oneOf(topicPartitionList)
        key <- Gen.alphaStr
        value <- Gen.alphaStr
      } yield (tp, key, value))
      records <- tempRecords.zipWithIndex.map { case ((tp, key, value), i) =>
        KafkaRecord[String, String](tp, i.toLong, key, value) }
    } yield records)

  implicit val byteArrayConsumerRecordsArb: Arbitrary[List[KafkaRecord[Array[Byte], Array[Byte]]]] =
    Arbitrary(for{
      topicPartitionList <- topicPartitionListArb.arbitrary
      tempRecords <- Gen.listOfN(1000, for {
        tp <- Gen.oneOf(topicPartitionList)
        key <- Gen.listOfN(128, Arbitrary.arbByte.arbitrary)
        value <- Gen.listOfN(256, Arbitrary.arbByte.arbitrary)
      } yield (tp, key, value))
      records <- tempRecords.zipWithIndex.map { case ((tp, key, value), i) =>
        KafkaRecord[Array[Byte], Array[Byte]](tp, i.toLong, key.toArray, value.toArray) }
    } yield records)


}
