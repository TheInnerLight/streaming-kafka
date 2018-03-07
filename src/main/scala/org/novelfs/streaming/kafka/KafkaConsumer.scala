package org.novelfs.streaming.kafka

import org.novelfs.streaming.kafka.ops._
import org.apache.kafka.clients.consumer.{Consumer => ApacheKafkaConsumer, KafkaConsumer => ConcreteApacheKafkaConsumer}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}
import fs2._
import cats.effect._
import cats.implicits._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._
import KafkaSdkConversions._

final case class KafkaConsumer[K, V](kafkaConsumer : ApacheKafkaConsumer[K, V])

object KafkaConsumer {

  private val log = LoggerFactory.getLogger(KafkaConsumer.getClass)

  private def identityPipe[F[_], O] : Pipe[F, O, O] = s => s

  /**
    * An effect to commit supplied map of offset metadata for each topic/partition pair
    */
  def commitOffsetMap[F[_] : Effect, K, V](consumer : KafkaConsumer[K, V])(offsetMap : Map[TopicPartition, OffsetMetadata]): F[Unit] =
    Async[F].async { (cb: Either[Throwable, Unit] => Unit) =>
      consumer.kafkaConsumer.commitAsync(offsetMap.toKafkaSdk, (_: java.util.Map[_, _], exception: Exception) => Option(exception) match {
        case None =>
          log.debug(s"Offset committed: $offsetMap")
          cb(Right(()))
        case Some(ex) =>
          log.error("Error committing offset", ex)
          cb(Left(ex))
      })
    }

  /**
    * A pipe that accumulates the offset metadata for each topic/partition pair for the supplied input stream of Consumer Records
    */
  def accumulateOffsetMetadata[F[_], K, V]: Pipe[F, KafkaRecord[K, V], (KafkaRecord[K, V], Map[TopicPartition, OffsetMetadata])] =
    _.zipWithScan(Map.empty[TopicPartition, OffsetMetadata])((map, record) => map + (record.topicPartition -> OffsetMetadata(record.offset)))

  /**
    * A pipe that commits the final available offset data for each topic/partition pair for the supplied input stream of Consumer Records
    */
  def commitFinalOffsets[F[_] : Effect, K, V](consumer : KafkaConsumer[K, V]) : Pipe[F, (KafkaRecord[K, V], Map[TopicPartition, OffsetMetadata]), (KafkaRecord[K, V], Map[TopicPartition, OffsetMetadata])] =
    _.noneTerminate
      .zipWithPrevious
      .observe1 {
        case (Some(Some((_, offsetMap))), None) => commitOffsetMap(consumer)(offsetMap)
        case _ => Effect[F].unit
      }
      .map {case (_, valueAndOffsetMap) => valueAndOffsetMap}
      .unNoneTerminate

  /**
    * A convenience pipe that accumulates offset metadata based on the supplied commitSettings and commits them to Kafka at some defined frequency
    */
  def commitOffsets[F[_] : Effect, K, V](consumer : KafkaConsumer[K, V])(autoCommitSettings: KafkaOffsetCommitSettings.AutoCommit)(implicit ex : ExecutionContext): Pipe[F, KafkaRecord[K,V], KafkaRecord[K,V]] =
      _.through(accumulateOffsetMetadata)
        .observeAsync(autoCommitSettings.maxAsyncCommits)(s =>
            s.through(commitFinalOffsets(consumer))
              .takeElementsEvery(autoCommitSettings.timeBetweenCommits)
              .evalMap {case (_, offsetMap) => commitOffsetMap(consumer)(offsetMap)})
        .map{case (r,_) => r}

  /**
    * An effect that generates a subscription to some Kafka topics/paritions using the supplied kafka config
    */
  def subscribeToConsumer[F[_] : Async, K, V](config : KafkaConsumerConfig[K, V]): F[KafkaConsumer[Array[Byte], Array[Byte]]] = {
    val consumer = new ConcreteApacheKafkaConsumer(KafkaConsumerConfig.generateProperties(config), new ByteArrayDeserializer(), new ByteArrayDeserializer())
    Async[F].delay (consumer.subscribe(config.topics.asJava)) *> Async[F].point(KafkaConsumer(consumer))
  }

  /**
    * An effect that disposes of some supplied kafka consumer
    */
  def cleanupConsumer[F[_] : Async, K, V](consumer : KafkaConsumer[K, V]): F[Unit] = Async[F].delay(consumer.kafkaConsumer.close())

  /**
    * An effect that polls kafka (once) with a supplied timeout
    */
  def pollKafka[F[_] : Async, K, V](consumer : KafkaConsumer[K, V])(pollTimeout : FiniteDuration): F[Vector[KafkaRecord[K, V]]] =
    Async[F].delay(consumer.kafkaConsumer.poll(pollTimeout.toMillis).fromKafkaSdk)

  /**
    * A pipe that deserialises an array of bytes using supplied key and value deserialisers
    */
  def deserializer[F[_] : Async, K, V](keyDeserializer: Deserializer[K], valueDeserializer : Deserializer[V]) : Pipe[F, KafkaRecord[Array[Byte], Array[Byte]], Either[Throwable, KafkaRecord[K, V]]] =
    _.evalMap(record =>
      Async[F].delay {
        val key = keyDeserializer.deserialize(record.topicPartition.topic, record.key)
        val value = valueDeserializer.deserialize(record.topicPartition.topic, record.value)
        record.copy(key = key, value = value)
      }.attempt
    )

  def topicPartitionAssignments[F[_] : Async, K, V](consumer : KafkaConsumer[K, V]): F[Set[TopicPartition]] =
    Async[F].delay { consumer.kafkaConsumer.assignment().fromKafkaSdk }

  /**
    * Creates a streaming subscription using the supplied kafka configuration
    */
  def apply[F[_] : Effect, K, V](config : KafkaConsumerConfig[K, V])(implicit ex : ExecutionContext): Stream[F, Either[Throwable, KafkaRecord[K, V]]] =
    Stream.bracket(subscribeToConsumer(config))(consumer =>
      for {
        records <- Stream.repeatEval(pollKafka(consumer)(config.pollTimeout))
        process <- Stream.emits(records)
          .covary[F]
          .through(config.commitOffsetSettings match {
            case autoCommitSettings : KafkaOffsetCommitSettings.AutoCommit => commitOffsets(consumer)(autoCommitSettings)
            case _ => identityPipe
          })
          .through(deserializer(config.keyDeserializer, config.valueDeserializer))
      } yield process, cleanupConsumer[F, Array[Byte], Array[Byte]])
}
