package org.novelfs.streaming.kafka

case class KafkaEncryptionSettings(trustStoreLocation : String, trustStorePassword : String)

case class KafkaAuthenticationSettings(keyStoreLocation : String, keyStorePassword : String, keyPassword : String)

sealed trait KafkaSecuritySettings
object KafkaSecuritySettings {
  final case object NoSecurity extends KafkaSecuritySettings
  final case class EncryptedNotAuthenticated(encryptionSettings: KafkaEncryptionSettings) extends KafkaSecuritySettings
  final case class AuthenticatedNotEncrypted(authSettings: KafkaAuthenticationSettings) extends KafkaSecuritySettings
  final case class EncryptedAndAuthenticated(encryptionSettings: KafkaEncryptionSettings, authSettings: KafkaAuthenticationSettings) extends KafkaSecuritySettings
}
