package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.exception.UnsupportedOperationException
import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceKafka.{YamlConfigSourceKafkaListTopics, YamlConfigSourceKafkaTopic, YamlConfigSourceKafkaTopicsByRegexp}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate
import pro.datawiki.yamlConfiguration.LogicClass

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigSourceKafka(
                                  topics: YamlConfigSourceKafkaTopic,
                                  listTopics: YamlConfigSourceKafkaListTopics,
                                  topicsByRegexp: YamlConfigSourceKafkaTopicsByRegexp
                                ) extends LogicClass, YamlConfigSourceTrait {
  @JsonIgnore
  def getLogic: Any = {
    super.getLogic(topics, listTopics, topicsByRegexp)
  }

  @JsonIgnore
  override def getTaskTemplate(connection: ConnectionTrait): TaskTemplate = {
    getLogic match
      case x: YamlConfigSourceKafkaTopic => return x.getTaskTemplate(connection)
      case x: YamlConfigSourceKafkaTopicsByRegexp => return x.getTaskTemplate(connection)
      case x: YamlConfigSourceKafkaListTopics => return x.getTaskTemplate(connection)
      case _ => throw UnsupportedOperationException(s"Unsupported Kafka source configuration type: ${getLogic.getClass.getSimpleName}")
  }
}