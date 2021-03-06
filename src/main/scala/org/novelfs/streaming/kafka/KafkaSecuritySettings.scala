package org.novelfs.streaming.kafka

import java.util.Properties

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs

/**
  * @param trustStoreLocation The location of the trust store file
  * @param trustStorePassword The password for the trust store file
  */
case class KafkaEncryptionSettings(trustStoreLocation : String, trustStorePassword : String)

/**
  * @param keyStoreLocation The location of the key store file
  * @param keyStorePassword The store password for the key store file
  * @param keyPassword The password of the private key in the key store file
  */
case class KafkaAuthenticationSettings(keyStoreLocation : String, keyStorePassword : String, keyPassword : Option[String], keystoreType : Option[String])

sealed trait KafkaSecuritySettings
object KafkaSecuritySettings {
  final case object NoSecurity extends KafkaSecuritySettings
  final case class EncryptedNotAuthenticated(encryptionSettings: KafkaEncryptionSettings) extends KafkaSecuritySettings
  final case class AuthenticatedNotEncrypted(authSettings: KafkaAuthenticationSettings) extends KafkaSecuritySettings
  final case class EncryptedAndAuthenticated(encryptionSettings: KafkaEncryptionSettings, authSettings: KafkaAuthenticationSettings) extends KafkaSecuritySettings

  def addEncryptionProps(props: Properties)(encryptionSettings: KafkaEncryptionSettings): Properties = {
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
    props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, encryptionSettings.trustStoreLocation)
    props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, encryptionSettings.trustStorePassword)
    props
  }

  def addAuthenticationProps(props: Properties)(authSettings: KafkaAuthenticationSettings): Properties = {
    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, authSettings.keyStoreLocation)
    props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, authSettings.keyStorePassword)
    authSettings.keyPassword match {
      case Some(password) => props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, password)
      case _              => ()
    }
    authSettings.keystoreType match {
      case Some(keystoreType) => props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, keystoreType)
      case _                  => ()
    }
    props
  }
}
