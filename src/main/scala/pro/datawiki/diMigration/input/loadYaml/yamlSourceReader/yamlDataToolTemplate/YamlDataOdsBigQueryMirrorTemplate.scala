package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceBigQuery, YamlDataTemplateSourceDBTable, YamlDataTemplateSourceFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.{YamlDataTemplateConnect, YamlDataTemplateSource, YamlDataTemplateTarget, YamlDataTemplateTransformation}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport.{YamlDataEtlToolTemplateSupportOds, YamlDataEtlToolTemplateSupportStg}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate}
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, InitModeEnum}
import pro.datawiki.yamlConfiguration.YamlClass

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
                                              metadataConfigLocation: String = throw Exception(),
                                              business_date: String,
                                              extra_code: String
                                            ) extends YamlDataTaskToolTemplate {

  override def isRunFromControlDag: Boolean = true

  val metadata = Metadata(metadataConnection, metadataConfigLocation, s"ods__$targetTableSchema", targetTableName)

  val support =
    YamlDataEtlToolTemplateSupport(
      taskName = taskName,
      sourceCode = "bigQuery",
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

  val stgSupport = new YamlDataEtlToolTemplateSupportStg(
    taskName = taskName,
    sourceLogicTableSchema = targetTableSchema,
    sourceLogicTableName = tableName,
    yamlFileCoreLocation = yamlFileCoreLocation,
    yamlFileLocation = yamlFileLocation,
    sourceCode = "kafka")

  val odsSupport = new YamlDataEtlToolTemplateSupportOds(
    taskName = taskName,
    tableSchema = targetTableSchema,
    tableName = targetTableName,
    metadata = metadata,
    yamlFileCoreLocation = yamlFileCoreLocation,
    yamlFileLocation = yamlFileLocation)

  override def getCoreTask: List[CoreTask] = {

    val stg = new YamlDataEtlToolTemplate(
      taskName = stgSupport.getStgTaskName,
      yamlFile = stgSupport.getStgYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(
        YamlDataTemplateSource(
          sourceName = support.getBigQuery(configLocation),
          objectName = "src",
          bigQuery = YamlDataTemplateSourceBigQuery(
            projectId = projectId,
            datasetId = datasetId,
            tableId = tableId
          ),
          initMode = InitModeEnum.instantly.toString
        )
      ),
      transform = List.apply(stgSupport.getExtractAndValidate("schema", "src")),
      target = List.apply(stgSupport.getStgYamlDataTemplateTarget(support.getParquetDataWarehouse, "src", List.apply("run_id"))),
      postEtlOperations = List.empty,
      dependencies = List.empty
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = support.getOdsTaskName,
      yamlFile = support.getOdsYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(stgSupport.getOdsYamlDataTemplateSource(support.getParquetDataWarehouse)),
      transform = List.empty,
      target = List.apply(support.getOdsYamlDataTemplateTarget(metadata,support.getMainDataWarehouse)),
      postEtlOperations = List.empty,
      dependencies = List.apply(stg.getTaskName)
    )

    val clickhouse = new YamlDataEtlToolTemplate(
      taskName = support.getClickhouseTaskName,
      yamlFile = support.getClickhouseYamlFile,
      preEtlOperations = List.apply(support.getClickhouseYamlConfigEltOnServerOperation(metadata)),
      sources = List.apply(support.getDataForDM(support.getPostgres)),
      transform = List.empty,
      target = List.apply(support.getClickhouseTarget(support.getClickhouseDataWarehouse, "src", metadata)),
      postEtlOperations = List.apply(support.getClickhouseYamlConfigEltOnServerOperationPost(metadata)),
      dependencies = List.apply(ods.getTaskName)
    )

    return stg.getCoreTask ++ ods.getCoreTask ++ clickhouse.getCoreTask
  }
}


object YamlDataOdsBigQueryMirrorTemplate extends YamlClass {
  def apply(inConfig: String, row: Map[String, String]): YamlDataOdsBigQueryMirrorTemplate = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsBigQueryMirrorTemplate = mapper.readValue(text, classOf[YamlDataOdsBigQueryMirrorTemplate])
    return configYaml
  }
}