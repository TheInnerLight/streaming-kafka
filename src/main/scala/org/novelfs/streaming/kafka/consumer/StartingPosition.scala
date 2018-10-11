package org.novelfs.streaming.kafka.consumer

import org.novelfs.streaming.kafka.TopicPartition

sealed trait StartingPosition

object StartingPosition {
  final case object Beginning extends StartingPosition
  final case object End extends StartingPosition
  final case class Explicit(position : Map[TopicPartition, OffsetMetadata]) extends StartingPosition
  final case object LastCommitted extends StartingPosition
}
