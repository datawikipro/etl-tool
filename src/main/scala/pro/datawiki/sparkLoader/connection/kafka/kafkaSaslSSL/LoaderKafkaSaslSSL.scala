package pro.datawiki.sparkLoader.connection.kafka.kafkaSaslSSL

import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions}
import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.kafka.kafkaBase
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

class LoaderKafkaSaslSSL(configYaml: YamlConfig, configLocation: String) extends kafkaBase.LoaderKafka(kafkaBase.YamlConfig(`bootstrap.servers` = configYaml.`bootstrap.servers`), configLocation), ConnectionTrait, QueryTrait {

  configs += ("security.protocol", "SASL_SSL")
  configs += ("sasl.mechanism", "SCRAM-SHA-256")
  configs += ("ssl.client.authd", "required")
  configs += ("ssl.endpoint.identification.algorithm", "")
  configs += ("ssl.truststore.type", configYaml.`ssl.truststore.type`)
  configs += ("ssl.truststore.certificates", configYaml.`ssl.truststore.certificates`)
  configs += ("sasl.jaas.config", """org.apache.kafka.common.security.scram.ScramLoginModule required username='user1' password='ooGh1eegue3ieb1aeth2' serviceName='kafka';""")

  override def getCacheKey(): String = {
    ConnectionTrait.generateCacheKey("kafkaSaslSSL", configLocation)
  }
}

object LoaderKafkaSaslSSL extends YamlClass {
  def apply(inConfig: String): LoaderKafkaSaslSSL = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderKafkaSaslSSL(configYaml, inConfig)
  }
}
