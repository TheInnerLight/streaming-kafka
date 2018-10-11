package org.novelfs.streaming.kafka.algebra

import org.novelfs.streaming.kafka.TopicPartition
import org.novelfs.streaming.kafka.consumer.{ConsumerRecord, OffsetMetadata}

import scala.concurrent.duration.FiniteDuration

trait KafkaConsumerAlg[F[_], TContext[_, _]] {

  /**
    * An effect to commit supplied map of offset metadata for each topic/partition pair
    */
  def commitOffsetMap[K, V](offsetMap : Map[TopicPartition, OffsetMetadata])(context: TContext[K, V]): F[Unit]

  /**
    * An effect that polls kafka (once) with a supplied timeout
    */
  def poll[K, V](pollTimeout : FiniteDuration)(context: TContext[K, V]) : F[Vector[ConsumerRecord[K, V]]]

  /**
    * An effect that seeks to the supplied offset for each of the given partitions.
    */
  def seekTo[K, V](topicPartitionOffsets : Map[TopicPartition, OffsetMetadata])(context: TContext[K, V]) : F[Unit]

  /**
    * An effect that seeks to the first offset for each of the given partitions.
    */
  def seekToBeginning[K, V](topicPartitions: Set[TopicPartition])(context: TContext[K, V]) : F[Unit]

  /**
    * An effect that seeks to the last offset for each of the given partitions.
    */
  def seekToEnd[K, V](topicPartitions: Set[TopicPartition])(context: TContext[K, V]) : F[Unit]

  /**
    * An effect to return the set of topic and partition assignments attached to the supplied consumer
    */
  def topicPartitionAssignments[K, V](context: TContext[K, V]): F[Set[TopicPartition]]

}
