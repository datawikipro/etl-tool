package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.diMigration.core.task.{CoreTask, CoreTaskEtlToolTemplate}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.YamlDataTaskToolTemplate
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.{YamlDataTemplateConnect, YamlDataTemplateSource, YamlDataTemplateTarget, YamlDataTemplateTransformation}
import pro.datawiki.sparkLoader.connection.kafka.kafkaSaslSSL.LoaderKafkaSaslSSL
import pro.datawiki.sparkLoader.connection.kafka.kafkaSaslSSL.LoaderKafkaSaslSSL.{getLines, mapper}
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlDataEtlToolTemplate(
                                    taskName: String,
                                    yamlFile: String,
                                    connections: List[YamlDataTemplateConnect] = List.apply(),
                                    sources: List[YamlDataTemplateSource] = List.apply(),
                                    transform: List[YamlDataTemplateTransformation] = List.apply(),
                                    target: List[YamlDataTemplateTarget] = List.apply(),
                                    dependencies: List[String] = List.apply(),
                                  ) extends YamlDataTaskToolTemplate {

  override def getCoreTask: List[CoreTask] = {

    return List.apply(
      CoreTaskEtlToolTemplate(
        taskName = taskName,
        yamlFile = yamlFile,
        connections = connections.map(col => col.getCoreConnection),
        sources = sources.map(col => col.getCoreSource),
        transform = transform.map(col => col.getCoreTransformation),
        target = target.map(col => col.getCoreTarget),
        dependencies=dependencies
      ),

    )
  }
}

object YamlDataEtlToolTemplate extends YamlClass {
  def apply(inConfig: String, row: mutable.Map[String, String]): YamlDataEtlToolTemplate = {
    val configYaml: YamlDataEtlToolTemplate = mapper.readValue(getLines(inConfig, row), classOf[YamlDataEtlToolTemplate])
    return configYaml
  }
}
