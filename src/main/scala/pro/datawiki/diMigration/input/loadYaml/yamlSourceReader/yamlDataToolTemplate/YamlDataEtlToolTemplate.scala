package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import com.fasterxml.jackson.annotation.JsonInclude
import pro.datawiki.diMigration.core.task.{CoreTask, CoreTaskEtlToolTemplate}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.YamlDataTaskToolTemplate
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.*
import pro.datawiki.sparkLoader.connection.kafka.kafkaSaslSSL.LoaderKafkaSaslSSL
import pro.datawiki.sparkLoader.connection.kafka.kafkaSaslSSL.LoaderKafkaSaslSSL.{getLines, mapper}
import pro.datawiki.yamlConfiguration.YamlClass

@JsonInclude(JsonInclude.Include.NON_ABSENT)
class YamlDataEtlToolTemplate(
                               taskName: String,
                               yamlFile: String,
                               preEtlOperations: List[YamlConfigEltOnServerOperation],
                               sources: List[YamlDataTemplateSource],
                               transform: List[YamlDataTemplateTransformation],
                               target: List[YamlDataTemplateTarget],
                               postEtlOperations: List[YamlConfigEltOnServerOperation],
                               dependencies: List[String],
                             ) extends YamlDataTaskToolTemplate {

  def getTaskName: String = taskName

  override def getCoreTask: List[CoreTask] = {
    return List.apply(
      CoreTaskEtlToolTemplate(
        taskName = taskName,
        yamlFile = yamlFile,
        preEtlOperations = preEtlOperations.map(col => col.getCorePreEtlOperations),
        connections =(sources.map(col => col.sourceName.getCoreConnection) ::: target.map(col => col.getCoreConnection)).distinct,
        sources = sources.map(col => col.getCoreSource),
        transform = transform.map(col => col.getCoreTransformation),
        target = target.map(col => col.getCoreTarget),
        postEtlOperations = postEtlOperations.map(col => col.getCorePreEtlOperations),
        dependencies = dependencies.map(col => col)
      ),

    )
  }

  override def isRunFromControlDag: Boolean = throw Exception()
}

object YamlDataEtlToolTemplate extends YamlClass {
  def apply(inConfig: String, row: Map[String, String]): YamlDataEtlToolTemplate = {
    val configYaml: YamlDataEtlToolTemplate = mapper.readValue(getLines(inConfig, row), classOf[YamlDataEtlToolTemplate])
    return configYaml
  }
}
