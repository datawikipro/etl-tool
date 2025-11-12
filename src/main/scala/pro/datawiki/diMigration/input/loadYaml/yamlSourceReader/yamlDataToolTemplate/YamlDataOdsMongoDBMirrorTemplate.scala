package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.*
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.YamlDataTemplateTransformationExtractAndValidateDataFrame
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport.{YamlDataEtlToolTemplateSupportOds, YamlDataEtlToolTemplateSupportStg}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate}
import pro.datawiki.sparkLoader.connection.NoSQLDatabaseTrait
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.yamlConfiguration.YamlClass

case class YamlDataOdsMongoDBMirrorTemplate(
                                             taskName: String,
                                             yamlFileCoreLocation: String,
                                             yamlFileLocation: String,
                                             connection: String,
                                             configLocation: String,
                                             metadataConnection: String,
                                             metadataConfigLocation: String,
                                             sourceSchema: String,
                                             sourceTable: String,
                                             tableSchema: String,
                                             tableName: String,
                                             business_date: String,
                                             extra_code: String,
                                             dwhConfigLocation: String
                                           ) extends YamlDataTaskToolTemplate {

  override def isRunFromControlDag: Boolean = true

  override def getCoreTask: List[CoreTask] = {
    val connectionTrait = NoSQLDatabaseTrait.apply(connection, configLocation)
    val metadata = Metadata(metadataConnection, metadataConfigLocation, s"ods__$tableSchema", tableName)
    val dwhLoader = LoaderPostgres(dwhConfigLocation)
    val dwhInfo = dwhLoader.getDwhConnectionInfo

    val support = new YamlDataEtlToolTemplateSupport(
      taskName = taskName,
      sourceCode = "monga",
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
      sourceCode = "monga")

    val odsSupport = new YamlDataEtlToolTemplateSupportOds(
      taskName = taskName,
      tableSchema = tableSchema,
      tableName = tableName,
      metadata = metadata,
      yamlFileCoreLocation = yamlFileCoreLocation,
      yamlFileLocation = yamlFileLocation)

    val stg = new YamlDataEtlToolTemplate(
      taskName = stgSupport.getStgBatchTaskName,
      yamlFile = stgSupport.getStgYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(stgSupport.getSrcFromRemote(stgSupport.getMonga(configLocation),sourceSchema, sourceTable)),
      transform = List.apply(
        stgSupport.getExtractAndValidate("schema","src"),
      ),
      target = List.apply(stgSupport.getStgYamlDataTemplateTarget(support.getJsonDataWarehouse, "src", List.empty)),
      postEtlOperations = List.empty,
      dependencies = List.empty
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = support.getOdsBatchTaskName,
      yamlFile = support.getOdsYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(stgSupport.getOdsYamlDataTemplateSource(support.getJsonDataWarehouse)),
      transform = List.apply(
        support.getSparkSqlTransformation("level1", s"""select * $extra_code from src"""),
        support.getSparkSqlTransformation("level2",
          s"""select ${metadata.columns.map(col => if col.column_name == "id" then s"_id as id" else s"${col.column_name} as ${col.column_name}").mkString(",")}
             |  from level1""".stripMargin)
      ),
      target = List.apply(odsSupport.writeOds(support.getPostgres,metadata.getWriteMode, "level2")),
      postEtlOperations = List.apply(support.getClickhouseYamlConfigEltOnServerOperationPost(metadata)),
      dependencies = List.apply(s"stg__batch__${taskName}")
    )

    val clickhouse = new YamlDataEtlToolTemplate(
      taskName = support.getClickhouseBatchTaskName,
      yamlFile = support.getClickhouseYamlFile,
      preEtlOperations = List.apply(support.getClickhouseYamlConfigEltOnServerOperation(metadata)),
      sources = List.apply(support.getDataForDM(support.getPostgres)),
      transform = List.apply(),
      target = List.apply(support.getClickhouseTarget(support.getClickhouseConfig, "src", metadata)),
      postEtlOperations = List.empty,
      dependencies = List.apply(ods.getTaskName)
    )

    val snowflake = new YamlDataEtlToolTemplate(
      taskName = support.getSnowflakeBatchTaskName,
      yamlFile = support.getSnowflakeYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(support.getDataForDM(support.getPostgres)),
      transform = List.apply(),
      target = List.apply(support.getSnowflakeTarget(support.getAmazonS3, "src")),
      postEtlOperations = List.empty,
      dependencies = List.apply(ods.getTaskName)
    )
    return stg.getCoreTask ++ ods.getCoreTask ++ snowflake.getCoreTask ++ clickhouse.getCoreTask
  }
}


object YamlDataOdsMongoDBMirrorTemplate extends YamlClass {
  def apply(inConfig: String, row: Map[String, String]): YamlDataOdsMongoDBMirrorTemplate = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsMongoDBMirrorTemplate = mapper.readValue(text, classOf[YamlDataOdsMongoDBMirrorTemplate])
    return configYaml
  }
}