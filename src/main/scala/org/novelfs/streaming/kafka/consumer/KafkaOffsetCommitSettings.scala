package org.novelfs.streaming.kafka.consumer


sealed trait KafkaOffsetCommitSettings
object KafkaOffsetCommitSettings {
  final case object DoNotCommit extends KafkaOffsetCommitSettings
  final case object AutoCommit extends KafkaOffsetCommitSettings
}
