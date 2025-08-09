package pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceKafka

import org.apache.spark.sql.Row
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait, QueryTrait}
import pro.datawiki.sparkLoader.task.*

case class YamlConfigSourceKafkaTopicsByRegexp(template: String) {
  def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = TaskTemplateKafkaLoadTopicByRegexp(template, connection)
}
