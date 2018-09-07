package org.novelfs.streaming.kafka.producer

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer
import org.novelfs.streaming.kafka.KafkaSecuritySettings

case class KafkaProducerConfig[K, V] private (
    brokers              : List[String],
    security             : KafkaSecuritySettings,
    keySerializer        : Serializer[K],
    valueSerializer      : Serializer[V]
    )

object KafkaProducerConfig {
  def apply[K, V](
               brokers              : List[String],
               security             : KafkaSecuritySettings,
               keySerializer        : Serializer[K],
               valueSerializer      : Serializer[V]) : KafkaProducerConfig[K, V] =
    new KafkaProducerConfig(
      brokers = brokers,
      security = security,
      keySerializer = keySerializer,
      valueSerializer = valueSerializer
    )

  def generateProperties[K, V](kafkaProducerConfig: KafkaProducerConfig[K, V]): Properties = {
    val props: Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerConfig.brokers.mkString(","))
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, new java.lang.Integer(0))
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, new java.lang.Integer(16384))
    props.put(ProducerConfig.LINGER_MS_CONFIG, new java.lang.Integer(1))
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, new java.lang.Integer(33554432))
    kafkaProducerConfig.security match {
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
