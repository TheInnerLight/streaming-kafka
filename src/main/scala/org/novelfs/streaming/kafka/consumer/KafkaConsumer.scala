package org.novelfs.streaming.kafka.consumer

import cats.effect._
import cats.implicits._
import fs2._
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}
import org.novelfs.streaming.kafka._
import org.slf4j.LoggerFactory
import org.novelfs.streaming.kafka.utils._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import cats.Functor
import fs2.concurrent.{Queue, Signal, SignallingRef}
import cats.effect.concurrent.MVar
import org.novelfs.streaming.kafka.interpreter.ThinKafkaConsumerClient

object KafkaConsumer {

  private val log = LoggerFactory.getLogger(KafkaConsumer.getClass)

  /**
    * A pipe that accumulates the offset metadata for each topic/partition pair for the supplied input stream of Consumer Records
    */
  def accumulateOffsetMetadata[F[_], K, V]: Pipe[F, ConsumerRecord[K, V], (ConsumerRecord[K, V], Map[TopicPartition, OffsetMetadata])] =
    _.zipWithScan1(Map.empty[TopicPartition, OffsetMetadata])((map, record) => map + (record.topicPartition -> OffsetMetadata(record.offset)))

  /**
    * A convenience pipe that accumulates offset metadata and publishes them to the supplied queue
    */
  def publishOffsetsToQueue[F[_] : Functor, K, V](queue : Queue[F, Map[TopicPartition, OffsetMetadata]]): Pipe[F, ConsumerRecord[K, V], ConsumerRecord[K, V]] =
    _.through(accumulateOffsetMetadata)
      .evalTap{case (_, offsetMap) => queue.enqueue1(offsetMap)}
      .map{case (value, _) => value}

  /**
    * A stream that commits the offsets in the supplied queue to kafka, using the supplied kafka consumer every supplied timeBetweenCommits
    */
  def commitOffsetsFromQueueEvery[F[_] : Concurrent : Timer, K, V](timeBetweenCommits : FiniteDuration)(lockedConsumer : MVar[F, KafkaConsumerSubscription[K, V]])(queue : Queue[F, Map[TopicPartition, OffsetMetadata]]): Stream[F, Unit] =
    for {
      obs <- queue.dequeue.hold(Map.empty[TopicPartition, OffsetMetadata])
      xs <- Stream
        .fixedRate(timeBetweenCommits)
        .evalMap(_ => obs.get)
        .zipWithPrevious
        .evalMap { case (maybePrevOffsetMap, currentOffsetMap) => {
          val commitMap = maybePrevOffsetMap.getOrElse(Map.empty).foldLeft(currentOffsetMap){ case (map, (tp, metadata)) =>
              map.get(tp) match {
                case Some(metadata2) if metadata2 == metadata => map - tp
                case _ => map
              }
            }
          lockedConsumer
            .locked(consumer =>
                ThinKafkaConsumerClient[F].commitOffsetMap(commitMap)(consumer)
            )
            .onError { case ex =>
              Sync[F].delay {
                log.error("Error during offset commit", ex)
              }
            }
        }
        }
    } yield xs

  /**
    * A pipe that deserialises an array of bytes using supplied key and value deserialisers
    */
  def deserializer[F[_] : Sync, K, V](keyDeserializer: Deserializer[K], valueDeserializer : Deserializer[V]) : Pipe[F, ConsumerRecord[Array[Byte], Array[Byte]], Either[Throwable, ConsumerRecord[K, V]]] =
    _.evalMap(record =>
      Sync[F].delay {
        val key = keyDeserializer.deserialize(record.topicPartition.topic, record.key)
        val value = valueDeserializer.deserialize(record.topicPartition.topic, record.value)
        record.copy(key = key, value = value)
      }.attempt
    )

  /**
    * A pipe that applies the kafka offset commit settings policy from the config
    */
  def applyCommitPolicy[F[_] : Concurrent : Timer, K, V](consumerVar : MVar[F, KafkaConsumerSubscription[Array[Byte], Array[Byte]]])(config : KafkaConsumerConfig[K, V]) : Pipe[F, ConsumerRecord[Array[Byte], Array[Byte]], ConsumerRecord[Array[Byte], Array[Byte]]] =
    stream =>
      config.commitOffsetSettings match {
        case KafkaOffsetCommitSettings.AutoCommit(timeBetweenCommits) =>
          for {
            queue <- Stream.eval(Queue.unbounded[F, Map[TopicPartition, OffsetMetadata]])
            mainStream = stream.through(publishOffsetsToQueue(queue))
            commitStream = commitOffsetsFromQueueEvery(timeBetweenCommits)(consumerVar)(queue).drain
            y <- Stream(mainStream, commitStream).parJoin(2)
          } yield y
        case _ => stream
      }

  /**
    * Creates a streaming subscription using the supplied kafka configuration
    */
  def apply[F[_] : Concurrent : Timer, K, V](config : KafkaConsumerConfig[K, V]): Stream[F, Either[Throwable, ConsumerRecord[K, V]]] = {
    val byteConfig = config.copy(keyDeserializer = new ByteArrayDeserializer(), valueDeserializer = new ByteArrayDeserializer())
    val thinKafkaConsumerClient = ThinKafkaConsumerClient[F]

    def subscribe: Resource[F, MVar[F, KafkaConsumerSubscription[Array[Byte], Array[Byte]]]] =
      for {
        consumer <- KafkaConsumerSubscription(byteConfig)
        result <- Resource.liftF(MVar.of(consumer))
      } yield result

    def assignStartingPosition(startingPosition: StartingPosition)(lockedConsumer : MVar[F, KafkaConsumerSubscription[Array[Byte], Array[Byte]]]) =
      lockedConsumer.locked(subscription =>
        for {
          topicPartitions <- thinKafkaConsumerClient.topicPartitionAssignments(subscription)
          _ <- startingPosition match {
            case StartingPosition.Beginning     => thinKafkaConsumerClient.seekToBeginning(topicPartitions)(subscription)
            case StartingPosition.End           => thinKafkaConsumerClient.seekToEnd(topicPartitions)(subscription)
            case StartingPosition.Explicit(pos) => thinKafkaConsumerClient.seekTo(pos)(subscription)
            case StartingPosition.LastCommitted => Sync[F].unit
          }
        } yield ()
      )

    def pollAndChunk(lockedConsumer : MVar[F, KafkaConsumerSubscription[Array[Byte], Array[Byte]]])(timeout : FiniteDuration): Stream[F, ConsumerRecord[Array[Byte], Array[Byte]]] =
      for {
        records <- Stream.eval(lockedConsumer.locked(consumer => thinKafkaConsumerClient.poll(config.pollTimeout)(consumer))).scope
        process <- Stream.chunk(Chunk.vector(records)).covary[F]
      } yield process

    Stream
      .resource(subscribe)
      .evalTap(lockedSubscription => assignStartingPosition(config.startingPosition)(lockedSubscription))
      .flatMap(lockedSubscription =>
        pollAndChunk(lockedSubscription)(config.initialConnectionTimeout)
          .append(pollAndChunk(lockedSubscription)(config.pollTimeout).repeat)
          .through(applyCommitPolicy(lockedSubscription)(config))
          .through(deserializer(config.keyDeserializer, config.valueDeserializer))

      )
  }
}
