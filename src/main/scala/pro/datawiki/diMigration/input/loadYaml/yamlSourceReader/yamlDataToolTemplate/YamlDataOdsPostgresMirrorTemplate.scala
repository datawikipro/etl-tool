package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.*
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceDBTable, YamlDataTemplateSourceFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.YamlDataTemplateTransformationExtractAndValidateDataFrame
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport.{YamlDataEtlToolTemplateSupportOds, YamlDataEtlToolTemplateSupportStg}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate}
import pro.datawiki.sparkLoader.connection.DatabaseTrait
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.dictionaryEnum.InitModeEnum
import pro.datawiki.yamlConfiguration.YamlClass

case class YamlDataOdsPostgresMirrorTemplate(
                                              taskName: String,
                                              yamlFileCoreLocation: String,
                                              yamlFileLocation: String,
                                              connection: String,
                                              configLocation: String,
                                              sourceSchema: String,
                                              sourceTable: String,
                                              tableSchema: String,
                                              tableName: String,
                                              metadataConnection: String,
                                              metadataConfigLocation: String,
                                              business_date: String,
                                              extra_code: String,
                                              dwhConfigLocation: String
                                            ) extends YamlDataTaskToolTemplate {

  override def isRunFromControlDag: Boolean = true

  val connectionTrait = DatabaseTrait.apply(connection, configLocation)

  val metadata = Metadata(metadataConnection, metadataConfigLocation, s"ods__$tableSchema", tableName)
  val support = new YamlDataEtlToolTemplateSupport(
    taskName = taskName,
    sourceCode = "postgres",
    sourceTableSchema = tableSchema,
    sourceTableName = tableName,
    sourceLogicTableSchema = tableSchema,
    sourceLogicTableName = tableName,
    targetTableSchema = tableSchema,
    targetTableName = tableName,
    connection = "kafka",
    yamlFileCoreLocation = yamlFileCoreLocation,
    yamlFileLocation = yamlFileLocation
  )

  val stgSupport = new YamlDataEtlToolTemplateSupportStg(
    taskName = taskName,
    sourceLogicTableSchema = tableSchema,
    sourceLogicTableName = tableName,
    yamlFileCoreLocation = yamlFileCoreLocation,
    yamlFileLocation = yamlFileLocation,
    sourceCode = "postgres")

  val odsSupport = new YamlDataEtlToolTemplateSupportOds(
    taskName = taskName,
    tableSchema = tableSchema,
    tableName = tableName,
    metadata = metadata,
    yamlFileCoreLocation = yamlFileCoreLocation,
    yamlFileLocation = yamlFileLocation)

  override def getCoreTask: List[CoreTask] = {
    val dwhLoader = LoaderPostgres(dwhConfigLocation)
    val dwhInfo = dwhLoader.getDwhConnectionInfo

    val stg = new YamlDataEtlToolTemplate(
      taskName = stgSupport.getStgBatchTaskName,
      yamlFile = stgSupport.getStgYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(
        YamlDataTemplateSource(
          stgSupport.getPostgres(configLocation),
          objectName = "src",
          sourceDb = YamlDataTemplateSourceDBTable(
            tableSchema = sourceSchema,
            tableName = sourceTable,
            tableColumns = List.apply(), //metadata.columns.map(col => YamlConfigSourceDBTableColumn(columnName = col.column_name)),
          ),
          initMode = InitModeEnum.instantly.toString
        )
      ),
      transform = List.apply(
        stgSupport.getExtractAndValidate("schema","src"),
      ),
      target = List.apply(
        stgSupport.getStgYamlDataTemplateTarget(support.getParquetDataWarehouse, "src", List.empty)
      ),
      postEtlOperations = List.empty,
      dependencies = List.empty
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = support.getOdsBatchTaskName,
      yamlFile = support.getOdsYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(stgSupport.getOdsYamlDataTemplateSource(support.getParquetDataWarehouse)),
      transform = List.apply(
        support.getSparkSqlTransformation("level1", s"""select * $extra_code from src"""),
      ),
      target = List.apply(odsSupport.writeOds(support.getPostgres,metadata.getWriteMode, "level1")),
      postEtlOperations = List.empty,
      dependencies = List.apply(s"stg__batch__${taskName}")
    )

    val clickhouse = new YamlDataEtlToolTemplate(
      taskName = s"clickhouse__batch__${taskName.replace("-", "_")}",
      yamlFile = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__postgres/clickhouse/$sourceTable.yaml",
      preEtlOperations = List.apply(support.getClickhouseYamlConfigEltOnServerOperation(metadata)),
      sources = List.apply(support.getDataForDM(support.getPostgres)),
      transform = List.empty,
      target = List.apply(support.getClickhouseTarget(support.getClickhouseConfig, "src", metadata)),
      postEtlOperations = List.apply(support.getClickhouseYamlConfigEltOnServerOperationPost(metadata)),
      dependencies = List.apply(ods.getTaskName)
    )

    val snowflake = new YamlDataEtlToolTemplate(
      taskName = support.getSnowflakeBatchTaskName,
      yamlFile = support.getSnowflakeYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(support.getDataForDM(support.getPostgres)),
      transform = List.empty,
      target = List.apply(support.getSnowflakeTarget(support.getAmazonS3, "src")),
      postEtlOperations = List.empty,
      dependencies = List.apply(ods.getTaskName)
    )
    return stg.getCoreTask ++ ods.getCoreTask ++ snowflake.getCoreTask ++ clickhouse.getCoreTask
  }
}


object YamlDataOdsPostgresMirrorTemplate extends YamlClass {
  def apply(inConfig: String, row: Map[String, String]): YamlDataOdsPostgresMirrorTemplate = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsPostgresMirrorTemplate = mapper.readValue(text, classOf[YamlDataOdsPostgresMirrorTemplate])
    return configYaml
  }
}