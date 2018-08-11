package org.novelfs.streaming.kafka.consumer

import scala.concurrent.duration.FiniteDuration

sealed trait KafkaOffsetCommitSettings
object KafkaOffsetCommitSettings {
  final case object DoNotCommit extends KafkaOffsetCommitSettings
  final case class AutoCommit(timeBetweenCommits: FiniteDuration) extends KafkaOffsetCommitSettings
}
