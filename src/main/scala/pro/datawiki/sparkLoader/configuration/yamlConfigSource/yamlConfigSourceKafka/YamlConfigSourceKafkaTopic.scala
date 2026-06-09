package pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceKafka

import org.apache.spark.sql.Row
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait, QueryTrait}
import pro.datawiki.sparkLoader.task.*
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateKafkaLoadTopicByList}

case class YamlConfigSourceKafkaTopic(topicList: List[String]) {
  def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = TaskTemplateKafkaLoadTopicByList(topicList, connection)
}
