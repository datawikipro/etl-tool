package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceBigQuery, YamlDataTemplateSourceDBTable, YamlDataTemplateSourceFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.{YamlDataTemplateConnect, YamlDataTemplateSource, YamlDataTemplateTarget, YamlDataTemplateTransformation}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate}
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, InitModeEnum}
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

case class YamlDataOdsBigQueryMirrorTemplate(
                                              taskName: String = throw Exception(),
                                              yamlFileCoreLocation: String = throw Exception(),
                                              yamlFileLocation: String = throw Exception(),
                                              configLocation: String = throw Exception(),
                                              projectId: String = throw Exception(),
                                              datasetId: String = throw Exception(),
                                              tableId: String = throw Exception(),
                                              tableName: String = throw Exception(),
                                              targetTableSchema: String = throw Exception(),
                                              targetTableName: String = throw Exception(),
                                              metadataConnection: String = throw Exception(),
                                              metadataConfigLocation: String = throw Exception()
                                            ) extends YamlDataTaskToolTemplate {
  val support =
    YamlDataEtlToolTemplateSupport(
      taskName = taskName,
      sourceTableSchema = datasetId,
      sourceTableName = tableId,
      sourceLogicTableSchema = targetTableSchema,
      sourceLogicTableName = tableName,
      targetTableSchema = targetTableSchema,
      targetTableName = targetTableName,
      yamlFileCoreLocation = yamlFileCoreLocation,
      yamlFileLocation = yamlFileLocation,
      connection = ConnectionEnum.bigQuery.toString
    )

  override def getCoreTask: List[CoreTask] = {
    val metadata = Metadata(metadataConnection, metadataConfigLocation, s"ods__$targetTableSchema", targetTableName)

    val stg = new YamlDataEtlToolTemplate(
      taskName = support.getStgTaskName,
      yamlFile = support.getStgYamlFile,
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "src",
          connection = ConnectionEnum.bigQuery.toString,
          configLocation = configLocation
        ),
        support.getParquetDataWarehouse
      ),
      preEtlOperations = List.empty,
      sources = List.apply(
        YamlDataTemplateSource(
          sourceName = "src",
          objectName = "src",
          bigQuery = YamlDataTemplateSourceBigQuery(
            projectId = projectId,
            datasetId = datasetId,
            tableId = tableId
          ),
          initMode = InitModeEnum.instantly.toString
        )
      ),
      transform = List.apply(support.getYamlDataTemplateTransformation),
      target = List.apply(support.getStgYamlDataTemplateTarget),
      dependencies = List.empty
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = support.getOdsTaskName,
      yamlFile = support.getOdsYamlFile,
      connections = List.apply(
        support.getParquetDataWarehouse,
        support.getMainDataWarehouse,
      ),
      preEtlOperations = List.empty,
      sources = List.apply(support.getOdsYamlDataTemplateSourceYamlDataTemplateSource),
      transform = List.apply(),
      target = List.apply(support.getOdsYamlDataTemplateTarget(metadata)),
      dependencies = List.apply(s"stg__batch__${taskName}")
    )

    return stg.getCoreTask ++ ods.getCoreTask
  }
}


object YamlDataOdsBigQueryMirrorTemplate extends YamlClass {
  def apply(inConfig: String, row: mutable.Map[String, String]): YamlDataOdsBigQueryMirrorTemplate = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsBigQueryMirrorTemplate = mapper.readValue(text, classOf[YamlDataOdsBigQueryMirrorTemplate])
    return configYaml
  }
}