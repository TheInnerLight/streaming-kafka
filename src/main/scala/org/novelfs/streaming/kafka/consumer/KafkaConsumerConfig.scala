package org.novelfs.streaming.kafka.consumer

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer
import org.novelfs.streaming.kafka._

import scala.concurrent.duration._

case class KafkaConsumerConfig[K, V](
    brokers              : List[String],
    security             : KafkaSecuritySettings,
    topics               : List[String],
    clientId             : String,
    groupId              : String,
    commitOffsetSettings : KafkaOffsetCommitSettings,
    pollTimeout          : FiniteDuration,
    maxPollRecords       : Int,
    keyDeserializer      : Deserializer[K],
    valueDeserializer    : Deserializer[V]
    )

object KafkaConsumerConfig {

  def apply[K, V](
             brokers: List[String],
             security: KafkaSecuritySettings,
             topics: List[String],
             clientId: String,
             groupId: String,
             keyDeserializer: Deserializer[K],
             valueDeserializer: Deserializer[V]
           ): KafkaConsumerConfig[K, V] =
    new KafkaConsumerConfig(
      brokers = brokers,
      security = security,
      topics = topics,
      clientId = clientId,
      groupId = groupId,
      commitOffsetSettings = KafkaOffsetCommitSettings.AutoCommit(500.milliseconds, 10),
      pollTimeout = 500.milliseconds,
      maxPollRecords = 100,
      keyDeserializer = keyDeserializer,
      valueDeserializer = valueDeserializer)

  def generateProperties[K, V](kafkaConsumerConfig: KafkaConsumerConfig[K, V]): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  kafkaConsumerConfig.brokers.mkString(","))
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   kafkaConsumerConfig.maxPollRecords.toString)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest")
    props.put(ConsumerConfig.CLIENT_ID_CONFIG,          kafkaConsumerConfig.clientId)
    props.put(ConsumerConfig.GROUP_ID_CONFIG,           kafkaConsumerConfig.groupId)
    kafkaConsumerConfig.security match {
      case KafkaSecuritySettings.EncryptedAndAuthenticated(encryptionSettings, authSettings) =>
        KafkaSecuritySettings.addEncryptionProps(KafkaSecuritySettings.addAuthenticationProps(props)(authSettings))(encryptionSettings)
      case KafkaSecuritySettings.EncryptedNotAuthenticated(encryptionSettings) =>
        KafkaSecuritySettings.addEncryptionProps(props)(encryptionSettings)
      case KafkaSecuritySettings.AuthenticatedNotEncrypted(authSettings) =>
        KafkaSecuritySettings.addAuthenticationProps(props)(authSettings)
      case KafkaSecuritySettings.NoSecurity =>
        props
    }
  }
}