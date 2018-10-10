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
  def commitOffsetsFromQueueEvery[F[_] : ConcurrentEffect : Timer, K, V](timeBetweenCommits : FiniteDuration)(lockedConsumer : MVar[F, KafkaConsumerSubscription[K, V]])(queue : Queue[F, Map[TopicPartition, OffsetMetadata]]): Stream[F, Unit] =
    for {
      obs <- queue.dequeue.hold(Map.empty[TopicPartition, OffsetMetadata])
      xs <- Stream
        .fixedRate(timeBetweenCommits)
        .evalMap(_ => obs.get)
        .evalMap { offsetMap =>
          lockedConsumer
            .locked(consumer => ThinKafkaConsumerClient[F].commitOffsetMap(offsetMap)(consumer))
            .onError { case ex =>
              Sync[F].delay{ log.error("Error during offset commit", ex) }
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
  def applyCommitPolicy[F[_] : ConcurrentEffect : Timer, K, V](consumerVar : MVar[F, KafkaConsumerSubscription[Array[Byte], Array[Byte]]])(config : KafkaConsumerConfig[K, V]) : Pipe[F, ConsumerRecord[Array[Byte], Array[Byte]], ConsumerRecord[Array[Byte], Array[Byte]]] =
    stream =>
      config.commitOffsetSettings match {
        case KafkaOffsetCommitSettings.AutoCommit(timeBetweenCommits) =>
          for {
            queue <- Stream.eval(Queue.unbounded[F, Map[TopicPartition, OffsetMetadata]])
            errorSignal <- Stream.eval(SignallingRef[F, Boolean](false))
            mainStream = stream.through(publishOffsetsToQueue(queue))
            commitStream = commitOffsetsFromQueueEvery(timeBetweenCommits)(consumerVar)(queue).drain
            y <- Stream(mainStream, commitStream).parJoin(2)
          } yield y
        case _ => stream
      }

  /**
    * Creates a streaming subscription using the supplied kafka configuration
    */
  def apply[F[_] : ConcurrentEffect : Timer, K, V](config : KafkaConsumerConfig[K, V]): Stream[F, Either[Throwable, ConsumerRecord[K, V]]] = {
    val byteConfig = config.copy(keyDeserializer = new ByteArrayDeserializer(), valueDeserializer = new ByteArrayDeserializer())
    val thinKafkaConsumerClient = ThinKafkaConsumerClient[F]

    def subscribe = for {
      consumer <- KafkaConsumerSubscription(byteConfig)
      result <- MVar.of(consumer)
    } yield result

    def pollAndChunk(lockedConsumer : MVar[F, KafkaConsumerSubscription[Array[Byte], Array[Byte]]])(timeout : FiniteDuration): Stream[F, ConsumerRecord[Array[Byte], Array[Byte]]] =
      for {
        records <- Stream.eval(lockedConsumer.locked(consumer => thinKafkaConsumerClient.poll(config.pollTimeout)(consumer))).scope
        process <- Stream.chunk(Chunk.vector(records)).covary[F]
      } yield process

    Stream
      .bracket(subscribe)(lockedConsumer => lockedConsumer.take.flatMap(KafkaConsumerSubscription.cleanup(_)))
      .flatMap(lockedConsumer =>
        pollAndChunk(lockedConsumer)(config.initialConnectionTimeout)
          .append(pollAndChunk(lockedConsumer)(config.pollTimeout).repeat)
          .through(applyCommitPolicy(lockedConsumer)(config))
          .through(deserializer(config.keyDeserializer, config.valueDeserializer))

      )
  }
}
