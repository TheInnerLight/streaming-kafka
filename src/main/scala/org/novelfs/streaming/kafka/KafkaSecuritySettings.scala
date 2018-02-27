package org.novelfs.streaming.kafka

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
case class KafkaAuthenticationSettings(keyStoreLocation : String, keyStorePassword : String, keyPassword : String)

sealed trait KafkaSecuritySettings
object KafkaSecuritySettings {
  final case object NoSecurity extends KafkaSecuritySettings
  final case class EncryptedNotAuthenticated(encryptionSettings: KafkaEncryptionSettings) extends KafkaSecuritySettings
  final case class AuthenticatedNotEncrypted(authSettings: KafkaAuthenticationSettings) extends KafkaSecuritySettings
  final case class EncryptedAndAuthenticated(encryptionSettings: KafkaEncryptionSettings, authSettings: KafkaAuthenticationSettings) extends KafkaSecuritySettings
}
