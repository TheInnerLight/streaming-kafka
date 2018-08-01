package org.novelfs.streaming.kafka.consumer

import cats.effect._
import cats.implicits._
import fs2._
import org.apache.kafka.clients.consumer.{Consumer => ApacheKafkaConsumer, KafkaConsumer => ConcreteApacheKafkaConsumer}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}
import org.novelfs.streaming.kafka._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import cats.Functor
import fs2.async.mutable.{Queue, Signal}
import KafkaSdkConversions._

final case class KafkaConsumer[K, V] private (kafkaConsumer : ApacheKafkaConsumer[K, V])

object KafkaConsumer {

  private val log = LoggerFactory.getLogger(KafkaConsumer.getClass)

  /**
    * An effect to commit supplied map of offset metadata for each topic/partition pair
    */
  def commitOffsetMap[F[_] : Effect, K, V](consumer : KafkaConsumer[K, V])(offsetMap : Map[TopicPartition, OffsetMetadata])(errorSignal : Signal[F, Boolean])(implicit ec: ExecutionContext): F[Unit] =
    Effect[F].delay {
      println(errorSignal)
      println(ec)
      consumer.kafkaConsumer.commitSync(offsetMap.toKafkaSdk)
      log.debug(s"Offset committed: $offsetMap")
      println(s"Offset committed: $offsetMap")
    }.onError{case ex => Effect[F].delay{println(ex)}}

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
  def commitOffsetsFromQueueEvery[F[_] : Effect, K, V](timeBetweenCommits : FiniteDuration)(consumer : KafkaConsumer[K, V])(queue : Queue[F, Map[TopicPartition, OffsetMetadata]])(implicit ec : ExecutionContext): Stream[F, Unit] =
    for {
      obs <- async.hold(Map.empty[TopicPartition, OffsetMetadata], queue.dequeue)
      errorSignal <- Stream.eval(async.signalOf[F, Boolean](false))
      xs <- Scheduler[F](corePoolSize = 2).flatMap(scheduler =>
        scheduler.fixedRate(timeBetweenCommits).evalMap(_ =>  {
            println("jodsjfdsf")
            obs.get
          })
        ).evalMap { offsetMap => commitOffsetMap(consumer)(offsetMap)(errorSignal) }
      //errorSignal <- Stream.eval(async.signalOf[F, Boolean](false))
      //xs <- queue.dequeue
//        .takeElementsEvery(timeBetweenCommits)
  //      .evalMap { offsetMap => commitOffsetMap(consumer)(offsetMap)(errorSignal) }
    } yield xs


  /**
    * An effect that generates a subscription to some Kafka topics/paritions using the supplied kafka config
    */
  def createConsumer[F[_] : Sync, K, V](config : KafkaConsumerConfig[K, V]): F[KafkaConsumer[Array[Byte], Array[Byte]]] =
    Sync[F].point(KafkaConsumer(new ConcreteApacheKafkaConsumer(KafkaConsumerConfig.generateProperties(config), new ByteArrayDeserializer(), new ByteArrayDeserializer())))

  /**
    * An effect that generates a subscription to some Kafka topics/paritions using the supplied kafka config
    */
  def subscribeToConsumer[F[_] : Sync, K, V](config : KafkaConsumerConfig[K, V]): F[KafkaConsumer[Array[Byte], Array[Byte]]] = {
    val consumer = new ConcreteApacheKafkaConsumer(KafkaConsumerConfig.generateProperties(config), new ByteArrayDeserializer(), new ByteArrayDeserializer())
    Sync[F].delay (consumer.subscribe(config.topics.asJava)) *> Sync[F].point(KafkaConsumer(consumer))
  }

  def subscribe[F[_] : Sync, K, V](config : KafkaConsumerConfig[K, V])(consumer : KafkaConsumer[K, V]): F[KafkaConsumer[K, V]] = {
    Sync[F].delay (consumer.kafkaConsumer.subscribe(config.topics.asJava)) *> Sync[F].point(consumer)
  }

  /**
    * An effect that disposes of some supplied kafka consumer
    */
  def cleanupConsumer[F[_] : Sync, K, V](consumer : KafkaConsumer[K, V]): F[Unit] =
    Sync[F].delay(consumer.kafkaConsumer.wakeup()) *>
      Sync[F].delay(consumer.kafkaConsumer.close())

  /**
    * An effect that polls kafka (once) with a supplied timeout
    */
  def pollKafka[F[_] : Sync, K, V](consumer : KafkaConsumer[K, V])(pollTimeout : FiniteDuration): F[Vector[ConsumerRecord[K, V]]] =
    Sync[F].delay(consumer.kafkaConsumer.poll(pollTimeout.toMillis).fromKafkaSdk)

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
    * An effect to return the set of topic and partition assignments attached to the supplied consumer
    */
  def topicPartitionAssignments[F[_] : Sync, K, V](consumer : KafkaConsumer[K, V]): F[Set[TopicPartition]] =
    Sync[F].delay { consumer.kafkaConsumer.assignment().fromKafkaSdk }


  /**
    * A pipe that applies the kafka offset commit settings policy from the config
    */
  def applyCommitPolicy[F[_] : Effect, K, V](consumer : KafkaConsumer[Array[Byte], Array[Byte]])(config : KafkaConsumerConfig[K, V])(implicit ex : ExecutionContext) : Pipe[F, ConsumerRecord[Array[Byte], Array[Byte]], ConsumerRecord[Array[Byte], Array[Byte]]] =
    stream => config.commitOffsetSettings match {
        case KafkaOffsetCommitSettings.AutoCommit(timeBetweenCommits, maxAsyncCommits) =>
          for {
            queue <- Stream.eval(Queue.bounded[F, Map[TopicPartition, OffsetMetadata]](maxAsyncCommits))
            y <- stream.through(publishOffsetsToQueue(queue))
              .concurrently(commitOffsetsFromQueueEvery(timeBetweenCommits)(consumer)(queue))
          } yield y
        case _ => stream
      }

  /**
    * Creates a streaming subscription using the supplied kafka configuration
    */
  def apply[F[_] : Effect, K, V](config : KafkaConsumerConfig[K, V])(implicit ex : ExecutionContext): Stream[F, Either[Throwable, ConsumerRecord[K, V]]] =
    Stream.bracket(subscribeToConsumer(config))(consumer =>
      for {
        records <- Stream.repeatEval(pollKafka(consumer)(config.pollTimeout)).scope
        process <- Stream.chunk(Chunk.vector(records))
          .covary[F]
          .through(applyCommitPolicy(consumer)(config))
          .through(deserializer(config.keyDeserializer, config.valueDeserializer))
      } yield process, cleanupConsumer[F, Array[Byte], Array[Byte]])
}
