package pro.datawiki.sparkLoader.connection.kafka.kafkaMSK

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.kafka.kafkaTemplate.LoaderKafkaTemplateBatch
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.ConnectionEnum
import pro.datawiki.yamlConfiguration.YamlClass

class LoaderKafkaMSK(configYaml: YamlConfig, configLocation: String) extends LoaderKafkaTemplateBatch(configYaml.`bootstrap.servers`,configYaml.offestConnector,  configYaml.offsetMetatable: String)  with ConnectionTrait with QueryTrait {

  configs += ("kafka.sasl.mechanism", "AWS_MSK_IAM")
  configs += ("kafka.security.protocol", "SASL_SSL")
  configs += ("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName=AdminDataPlatform;") //awsProfileName="platform-dev-kafka"
  configs += ("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")

  override def getCacheKey(): String = ConnectionTrait.generateCacheKey("kafkaAmazon", configLocation)

  private val _configLocation: String = configLocation

  override def getConfigLocation(): String = _configLocation

  override def getListTopics(template: String): DataFrame = super.getListTopics(template)

  override def getDataFrameFromTopic(topic: String): DataFrame = super.getDataFrameFromTopic(topic, None)

  override def close(): Unit = {
    super.close()
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = ConnectionEnum.kafkaBatch

}

object LoaderKafkaMSK extends YamlClass {
  def apply(inConfig: String): LoaderKafkaMSK = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderKafkaMSK(configYaml, inConfig)
  }
}