package pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceKafka

import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.task.*

case class YamlConfigSourceKafkaListTopics(template: String) {
  def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = TaskTemplateKafkaLoadListTopics(template, connection)
}
