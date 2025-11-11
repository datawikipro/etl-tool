package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceDBTable, YamlDataTemplateSourceFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.YamlDataTemplateTargetDummy
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.{YamlDataTemplateTransformationExtractAndValidateDataFrame, YamlDataTemplateTransformationSparkSql}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.*
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport.{YamlDataEtlToolTemplateSupportOds, YamlDataEtlToolTemplateSupportStg}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate}
import pro.datawiki.sparkLoader.connection.NoSQLDatabaseTrait
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.dictionaryEnum.{InitModeEnum, WriteMode}
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
      yamlFileLocation = yamlFileLocation,
      sourceCode = "monga")

    val stg = new YamlDataEtlToolTemplate(
      taskName = stgSupport.getStgBatchTaskName,
      yamlFile = stgSupport.getStgYamlFile,
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "mongodb",
          connection = "mongodb",
          configLocation = configLocation
        ),
        YamlDataTemplateConnect(
          sourceName = "datewarehouse",
          connection = "minioJson",
          configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
        ),
      ),
      preEtlOperations = List.empty,
      sources = List.apply(
        YamlDataTemplateSource(
          sourceName = "mongodb",
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
        YamlDataTemplateTransformation(
          objectName = "schema",
          cache = null,
          idMap = null,
          sparkSql = null,
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = YamlDataTemplateTransformationExtractAndValidateDataFrame(
            dataFrameIn = "src",
            configLocation = s"/opt/etl-tool/configMigrationSchemas/mongodb__${sourceSchema}__$sourceTable.yaml"
          ),
          adHoc = null,
          deduplicate = null
        ),
      ),
      target = List.apply(stgSupport.getStgYamlDataTemplateTarget("datewarehouse")),
      postEtlOperations = List.empty,
      dependencies = List.empty
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = s"ods__batch__${taskName.replace("-", "_")}",
      yamlFile = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__monga/ods/$sourceTable.yaml",
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "datewarehouse",
          connection = "minioJson",
          configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
        ),
        support.getPostgres,
      ),
      preEtlOperations = List.empty,
      sources = List.apply(stgSupport.getOdsYamlDataTemplateSourceYamlDataTemplateSource("datewarehouse")),
      transform = List.apply(
        YamlDataTemplateTransformation(
          objectName = "level1",
          cache = null,
          idMap = null,
          sparkSql = YamlDataTemplateTransformationSparkSql(
            sql = s"select * $extra_code from src",
          ),
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = null,
          adHoc = null,
          deduplicate = null
        ),
        YamlDataTemplateTransformation(
          objectName = "level2",
          cache = null,
          idMap = null,
          sparkSql = YamlDataTemplateTransformationSparkSql(
            sql = s"select ${
              metadata.columns.map(col => {
                if col.column_name == "id" then s"_id as id" else s"${col.column_name} as ${col.column_name}"
              }).mkString(",")
            } from level1",
          ),
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = null,
          adHoc = null,
          deduplicate = null
        ),
      ),
      target = List.apply(
        odsSupport.writeOds(metadata.columns.isEmpty match {
          case true => WriteMode.overwritePartition
          case false => WriteMode.mergeFull
        }, "level2")
      ),
      postEtlOperations =List.apply(support.getClickhouseYamlConfigEltOnServerOperationPost(metadata)),
      dependencies = List.apply(s"stg__batch__${taskName}")
    )

    val clickhouse = new YamlDataEtlToolTemplate(
      taskName = support.getClickhouseBatchTaskName,
      yamlFile = support.getClickhouseYamlFile,
      connections = List.apply(
        support.getPostgres,support.getClickhouseConfig),
      preEtlOperations = List.apply(support.getClickhouseYamlConfigEltOnServerOperation(metadata)),
      sources = List.apply(support.getDataForDM),
      transform = List.apply(),
      target = List.apply(support.getClickhouseTarget("clickhouseUnico", "src", metadata)      ),
      postEtlOperations = List.empty,
      dependencies = List.apply(ods.taskName)
    )

    val snowflake = new YamlDataEtlToolTemplate(
      taskName = support.getSnowflakeBatchTaskName,
      yamlFile = support.getSnowflakeYamlFile,
      connections = List.apply(
        support.getPostgres,
        support.getAmazonS3,
      ),
      preEtlOperations = List.empty,
      sources = List.apply(support.getDataForDM),
      transform = List.apply(),
      target = List.apply(support.getSnowflakeTarget("datewarehouse", "src")),
      postEtlOperations = List.empty,
      dependencies = List.apply(ods.taskName)
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