package pro.datawiki.sparkLoader.connection.kafka.kafkaBase

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.kafka.kafkaTemplate.LoaderKafkaTemplateBatch
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, KafkaMetadataTrait, QueryTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.ConnectionEnum
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.yamlConfiguration.YamlClass
import org.apache.spark.sql.functions._


class LoaderKafka(configYaml: YamlConfig, configLocation: String) extends LoaderKafkaTemplateBatch(configYaml.`bootstrap.servers`,configYaml.offestConnector,  configYaml.offsetMetatable: String)  with ConnectionTrait with QueryTrait with LoggingTrait  {
  private val _configLocation: String = configLocation

  override def getConfigLocation(): String = _configLocation
  
  override def getListTopics(template: String): DataFrame = super.getListTopics(template)
  
  // Override to use offsets from metadata storage
  override def getDataFrameFromTopic(topic: String): DataFrame = {
    super.getDataFrameFromTopic(topic, readOffsets(topic))
  }
  

  override def close(): Unit = {
    logInfo(s"Successfully updated offsets")
    super.close()
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = ConnectionEnum.kafkaBatch

}

object LoaderKafka extends YamlClass {
  def apply(inConfig: String): LoaderKafka = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderKafka(configYaml, inConfig)
  }
}
