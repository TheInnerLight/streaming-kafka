package org.novelfs.streaming.kafka.consumer

import cats.effect._
import cats.implicits._
import fs2._
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}
import org.novelfs.streaming.kafka._
import org.slf4j.LoggerFactory
import org.novelfs.streaming.kafka.utils._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import cats.Functor
import fs2.async.mutable.{Queue, Signal}
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
      .observe1{case (_, offsetMap) => queue.enqueue1(offsetMap)}
      .map{case (value, _) => value}

  /**
    * A stream that commits the offsets in the supplied queue to kafka, using the supplied kafka consumer every supplied timeBetweenCommits
    */
  def commitOffsetsFromQueueEvery[F[_] : ConcurrentEffect, K, V](timeBetweenCommits : FiniteDuration)(lockedConsumer : MVar[F, KafkaConsumerSubscription[K, V]])(errorSignal : Signal[F, Boolean])(queue : Queue[F, Map[TopicPartition, OffsetMetadata]])(implicit ec : ExecutionContext): Stream[F, Unit] =
    for {
      obs <- async.hold(Map.empty[TopicPartition, OffsetMetadata], queue.dequeue)
      scheduler <- Scheduler[F](corePoolSize = 2)
      xs <- scheduler
        .fixedRate(timeBetweenCommits).evalMap(_ => obs.get)
        .evalMap { offsetMap =>
          lockedConsumer
            .locked(consumer => ThinKafkaConsumerClient[F].commitOffsetMap(offsetMap)(consumer))
            .handleErrorWith { case ex =>
              log.error("Error during offset commit", ex)
              errorSignal.set(true)
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
  def applyCommitPolicy[F[_] : ConcurrentEffect, K, V](consumerVar : MVar[F, KafkaConsumerSubscription[Array[Byte], Array[Byte]]])(config : KafkaConsumerConfig[K, V])(implicit ex : ExecutionContext) : Pipe[F, ConsumerRecord[Array[Byte], Array[Byte]], ConsumerRecord[Array[Byte], Array[Byte]]] =
    stream =>
      config.commitOffsetSettings match {
        case KafkaOffsetCommitSettings.AutoCommit(timeBetweenCommits) =>
          for {
            queue <- Stream.eval(Queue.unbounded[F, Map[TopicPartition, OffsetMetadata]])
            errorSignal <- Stream.eval(async.signalOf[F, Boolean](false))
            y <- stream.through(publishOffsetsToQueue(queue))
              .interruptWhen(errorSignal)
              .concurrently(commitOffsetsFromQueueEvery(timeBetweenCommits)(consumerVar)(errorSignal)(queue))
          } yield y
        case _ => stream
      }

  /**
    * Creates a streaming subscription using the supplied kafka configuration
    */
  def apply[F[_] : ConcurrentEffect, K, V](config : KafkaConsumerConfig[K, V])(implicit ex : ExecutionContext): Stream[F, Either[Throwable, ConsumerRecord[K, V]]] = {
    val byteConfig = config.copy(keyDeserializer = new ByteArrayDeserializer(), valueDeserializer = new ByteArrayDeserializer())

    def subscribe = for {
      consumer <- KafkaConsumerSubscription(byteConfig)
      result <- MVar.of(consumer)
    } yield result

    Stream.bracket(subscribe)(lockedConsumer => {
      val stream =
        for {
          records <- Stream.repeatEval(lockedConsumer.locked(consumer => ThinKafkaConsumerClient[F].poll(config.pollTimeout)(consumer))).scope
          process <- Stream.chunk(Chunk.vector(records)).covary[F]
        } yield process
      stream
        .through(applyCommitPolicy(lockedConsumer)(config))
        .through(deserializer(config.keyDeserializer, config.valueDeserializer))
    }, lockedConsumer => lockedConsumer.take.flatMap(KafkaConsumerSubscription.cleanup(_)))
  }
}
