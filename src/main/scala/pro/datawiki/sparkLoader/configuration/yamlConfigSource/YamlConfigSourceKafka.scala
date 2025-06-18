package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceKafka.{YamlConfigSourceKafkaListTopics, YamlConfigSourceKafkaTopic, YamlConfigSourceKafkaTopicsByRegexp}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.task.TaskTemplate
import pro.datawiki.yamlConfiguration.LogicClass

case class YamlConfigSourceKafka(
                                  topics: YamlConfigSourceKafkaTopic,
                                  listTopics: YamlConfigSourceKafkaListTopics,
                                  topicsByRegexp: YamlConfigSourceKafkaTopicsByRegexp
                                ) extends LogicClass, YamlConfigSourceTrait {

  def getLogic: Any = {
    super.getLogic(topics, listTopics, topicsByRegexp)
  }

  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    getLogic match
      case x: YamlConfigSourceKafkaTopic => return x.getTaskTemplate(connection)
      case x: YamlConfigSourceKafkaTopicsByRegexp => return x.getTaskTemplate(connection)
      case x: YamlConfigSourceKafkaListTopics => return x.getTaskTemplate(connection)
      case _ => throw Exception()
  }
}