package pro.datawiki.sparkLoader.connection.kafka.kafkaBase

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.kafka.kafkaTemplate.LoaderKafkaTemplateStream
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.ConnectionEnum
import pro.datawiki.yamlConfiguration.YamlClass


class LoaderKafkaStream(configYaml: YamlConfig, configLocation: String) extends LoaderKafkaTemplateStream(configYaml.`bootstrap.servers`)  with ConnectionTrait with QueryTrait  {
  private val _configLocation: String = configLocation

  override def getConfigLocation(): String = _configLocation
  
  override def getListTopics(template: String): DataFrame = super.getListTopics(template)

  override def getDataFrameFromTopic(topic: String): DataFrame = super.getDataFrameFromTopic(topic: String)

  override def close(): Unit = {
    super.close()
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = ConnectionEnum.kafkaStream

}

object LoaderKafkaStream extends YamlClass {
  def apply(inConfig: String): LoaderKafkaStream = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderKafkaStream(configYaml, inConfig)
  }
}
