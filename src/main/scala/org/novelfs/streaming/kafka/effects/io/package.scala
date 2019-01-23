package org.novelfs.streaming.kafka.effects

import cats.{Foldable, Monad}
import cats.implicits._
import cats.effect.{IO, LiftIO, Sync}
import cats.mtl.{ApplicativeAsk, ApplicativeLocal}
import org.novelfs.streaming.kafka.TopicPartition
import org.novelfs.streaming.kafka.consumer.{ConsumerRecord, KafkaConsumerSubscription, OffsetMetadata}
import org.novelfs.streaming.kafka.KafkaSdkConversions._
import org.novelfs.streaming.kafka.producer.{KafkaProducerSubscription, ProducerRecord}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration

package object io {
  implicit def ioMonadKafkaConsumer[F[_] : LiftIO : Monad] = new MonadKafkaConsumer[F, KafkaConsumerSubscription] {

    private val log = LoggerFactory.getLogger(this.getClass)

    /**
      * An effect to commit supplied map of offset metadata for each topic/partition pair
      */
    override def commitOffsetMap[K, V](offsetMap: Map[TopicPartition, OffsetMetadata])(context: KafkaConsumerSubscription[K, V]): F[Unit] =
      LiftIO[F].liftIO(IO {
        if(offsetMap.nonEmpty) {
          context.kafkaConsumer.commitSync(offsetMap.toKafkaSdk)
          log.debug(s"Offset committed: $offsetMap")
        } else {
          log.debug(s"Ignored empty offsetMap")
        }
      })

    /**
      * An effect that seeks to the supplied offset for each of the given partitions.
      */
    override def seekTo[K, V](topicPartitionOffsets : Map[TopicPartition, OffsetMetadata])(context: KafkaConsumerSubscription[K, V]) : F[Unit] =
      LiftIO[F].liftIO(IO {
        topicPartitionOffsets.foreach {
          case (tp, om) => context.kafkaConsumer.seek(tp.toKafkaSdk, om.toKafkaSdk.offset)
        }
      })

    /**
      * An effect that seeks to the first offset for each of the given partitions.
      */
    override def seekToBeginning[K, V](topicPartitions : Set[TopicPartition])(context: KafkaConsumerSubscription[K, V]) : F[Unit] =
      LiftIO[F].liftIO(IO { context.kafkaConsumer.seekToBeginning(topicPartitions.toKafkaSdk) })

    /**
      * An effect that seeks to the last offset for each of the given partitions.
      */
    override def seekToEnd[K, V](topicPartitions : Set[TopicPartition])(context: KafkaConsumerSubscription[K, V]) : F[Unit] =
      LiftIO[F].liftIO(IO { context.kafkaConsumer.seekToEnd(topicPartitions.toKafkaSdk) })

    /**
      * An effect that polls kafka (once) with a supplied timeout
      */
    override def poll[K, V](pollTimeout: FiniteDuration)(context: KafkaConsumerSubscription[K, V]): F[Vector[ConsumerRecord[K, V]]] =
      LiftIO[F].liftIO(IO { context.kafkaConsumer.poll(java.time.Duration.ofMillis(pollTimeout.toMillis)).fromKafkaSdk })

    /**
      * An effect to return the set of topic and partition assignments attached to the supplied consumer
      */
    override def topicPartitionAssignments[K, V](context: KafkaConsumerSubscription[K, V]): F[Set[TopicPartition]] =
      LiftIO[F].liftIO(IO { context.kafkaConsumer.assignment().fromKafkaSdk })
  }

  implicit def ioKafkaProducer[F[_] : LiftIO : Monad] = new MonadKafkaProducer[F] {
    override type TContext[A, B] = KafkaProducerSubscription[A, B]

    /**
      * An effect that sends a supplied producer record to the supplier kafka producer
      */
    override def send[K, V](record: ProducerRecord[K, V])(context: KafkaProducerSubscription[K, V]): F[Unit] =
      LiftIO[F].liftIO { IO { context.kafkaProducer.send(record.toKafkaSdk) } } *> Monad[F].unit

    /**
      * An effect that sends a supplied producer records to the supplier kafka producer
      */
    override def sendN[K, V, G[_] : Foldable](records: G[ProducerRecord[K, V]])(context: KafkaProducerSubscription[K, V]): F[Unit] =
      records.traverse_(record => send(record)(context))
  }
}
