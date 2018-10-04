package org.novelfs.streaming.kafka

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SslConfigs
import org.novelfs.streaming.kafka.consumer.KafkaConsumerConfig
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class KafkaConsumerConfigSpec extends FlatSpec with Matchers with MockFactory with GeneratorDrivenPropertyChecks with DomainArbitraries {

  "KafkaConsumerConfig.generateProperties" should "populate  properties correctly" in {
    forAll { config : KafkaConsumerConfig[String, String] =>
      KafkaConsumerConfig.generateProperties(config).getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) shouldBe config.brokers.mkString(",")
      KafkaConsumerConfig.generateProperties(config).getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG) shouldBe config.maxPollRecords.toString
      KafkaConsumerConfig.generateProperties(config).getProperty(ConsumerConfig.CLIENT_ID_CONFIG) shouldBe config.clientId
      KafkaConsumerConfig.generateProperties(config).getProperty(ConsumerConfig.GROUP_ID_CONFIG) shouldBe config.groupId
      config.security match  {
        case KafkaSecuritySettings.NoSecurity => {}
        case KafkaSecuritySettings.EncryptedNotAuthenticated(encryptionSettings) =>
          KafkaConsumerConfig.generateProperties(config).getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG) shouldBe "SSL"
          KafkaConsumerConfig.generateProperties(config).getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) shouldBe encryptionSettings.trustStoreLocation
          KafkaConsumerConfig.generateProperties(config).getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG) shouldBe encryptionSettings.trustStorePassword
        case KafkaSecuritySettings.AuthenticatedNotEncrypted(authSettings) =>
          KafkaConsumerConfig.generateProperties(config).getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG) shouldBe authSettings.keyStoreLocation
          KafkaConsumerConfig.generateProperties(config).getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG) shouldBe authSettings.keyStorePassword
          Option(KafkaConsumerConfig.generateProperties(config).getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG)) shouldBe authSettings.keyPassword
        case KafkaSecuritySettings.EncryptedAndAuthenticated(encryptionSettings, authSettings) =>
          KafkaConsumerConfig.generateProperties(config).getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG) shouldBe "SSL"
          KafkaConsumerConfig.generateProperties(config).getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) shouldBe encryptionSettings.trustStoreLocation
          KafkaConsumerConfig.generateProperties(config).getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG) shouldBe encryptionSettings.trustStorePassword
          KafkaConsumerConfig.generateProperties(config).getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG) shouldBe authSettings.keyStoreLocation
          KafkaConsumerConfig.generateProperties(config).getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG) shouldBe authSettings.keyStorePassword
          Option(KafkaConsumerConfig.generateProperties(config).getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG)) shouldBe authSettings.keyPassword
      }
    }
  }

}
