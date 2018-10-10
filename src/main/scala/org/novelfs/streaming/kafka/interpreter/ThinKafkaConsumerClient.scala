package org.novelfs.streaming.kafka.interpreter

import cats.effect.Sync
import org.novelfs.streaming.kafka.algebra.KafkaConsumerAlg
import org.novelfs.streaming.kafka.TopicPartition
import org.novelfs.streaming.kafka.consumer.{ConsumerRecord, KafkaConsumerSubscription, OffsetMetadata}
import org.slf4j.LoggerFactory
import org.novelfs.streaming.kafka.KafkaSdkConversions._

import scala.concurrent.duration.FiniteDuration

object ThinKafkaConsumerClient {

  /**
    * Create a KafkaConsumerAlg which interprets instructions using a thin wrapper around the Apache Kafka Consumer (consequently, this client is not threadsafe)
    */
  def apply[F[_] : Sync]: KafkaConsumerAlg[F, KafkaConsumerSubscription] = new KafkaConsumerAlg[F, KafkaConsumerSubscription] {

    private val log = LoggerFactory.getLogger(ThinKafkaConsumerClient.getClass)

    /**
      * An effect to commit supplied map of offset metadata for each topic/partition pair
      */
    override def commitOffsetMap[K, V](offsetMap: Map[TopicPartition, OffsetMetadata])(context: KafkaConsumerSubscription[K, V]): F[Unit] =
      Sync[F].delay {
        if(offsetMap.nonEmpty) {
          context.kafkaConsumer.commitSync(offsetMap.toKafkaSdk)
          log.debug(s"Offset committed: $offsetMap")
        } else {
          log.debug(s"Ignored empty offsetMap")
        }
      }

    /**
      * An effect that polls kafka (once) with a supplied timeout
      */
    override def poll[K, V](pollTimeout: FiniteDuration)(context: KafkaConsumerSubscription[K, V]): F[Vector[ConsumerRecord[K, V]]] =
      Sync[F].delay { context.kafkaConsumer.poll(java.time.Duration.ofMillis(pollTimeout.toMillis)).fromKafkaSdk }

    /**
      * An effect to return the set of topic and partition assignments attached to the supplied consumer
      */
    override def topicPartitionAssignments[K, V](context: KafkaConsumerSubscription[K, V]): F[Set[TopicPartition]] =
      Sync[F].delay { context.kafkaConsumer.assignment().fromKafkaSdk }
  }

}
