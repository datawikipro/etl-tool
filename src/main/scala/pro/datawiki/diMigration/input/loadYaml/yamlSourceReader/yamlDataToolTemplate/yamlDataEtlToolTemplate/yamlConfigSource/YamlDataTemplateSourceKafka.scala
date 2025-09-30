package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource

import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.yamlConfigSourceKafka.{YamlDataTemplateSourceKafkaListTopics, YamlDataTemplateSourceKafkaTopic, YamlDataTemplateSourceKafkaTopicsByRegexp}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceKafka.{YamlConfigSourceKafkaListTopics, YamlConfigSourceKafkaTopic, YamlConfigSourceKafkaTopicsByRegexp}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate
import pro.datawiki.yamlConfiguration.LogicClass

case class YamlDataTemplateSourceKafka(
                                        topics: YamlDataTemplateSourceKafkaTopic,
                                        listTopics: YamlDataTemplateSourceKafkaListTopics,
                                        topicsByRegexp: YamlDataTemplateSourceKafkaTopicsByRegexp
                                      )