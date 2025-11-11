package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.*
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceDBTable, YamlDataTemplateSourceFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetDummy, YamlDataTemplateTargetFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.{YamlDataTemplateTransformationExtractAndValidateDataFrame, YamlDataTemplateTransformationSparkSql}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport.{YamlDataEtlToolTemplateSupportOds, YamlDataEtlToolTemplateSupportStg}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate}
import pro.datawiki.sparkLoader.connection.DatabaseTrait
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, InitModeEnum, WriteMode}
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
    yamlFileLocation = yamlFileLocation,
    sourceCode = "postgres")

  override def getCoreTask: List[CoreTask] = {
    val dwhLoader = LoaderPostgres(dwhConfigLocation)
    val dwhInfo = dwhLoader.getDwhConnectionInfo

    val stg = new YamlDataEtlToolTemplate(
      taskName = stgSupport.getStgBatchTaskName,
      yamlFile = stgSupport.getStgYamlFile,
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "postgres",
          connection = "postgres",
          configLocation = configLocation
        ),
        YamlDataTemplateConnect(
          sourceName = "datewarehouse",
          connection = ConnectionEnum.minioParquet.toString,
          configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
        ),
      ),
      preEtlOperations = List.empty,
      sources = List.apply(
        YamlDataTemplateSource(
          support.getPostgres.getSourceName,
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
            configLocation = s"/opt/etl-tool/configMigrationSchemas/postgresdb__${sourceSchema}__$sourceTable.yaml"
          ),
          adHoc = null,
          deduplicate = null
        ),
      ),
      target = List.apply(
        YamlDataTemplateTarget(
          database = null,
          messageBroker = null,
          dummy = null,
          fileSystem = YamlDataTemplateTargetFileSystem(
            connection = "datewarehouse",
            source = "src",
            mode = WriteMode.overwritePartition,
            targetFile = s"stg/${tableSchema}/${sourceTable}",
            partitionBy = List.apply("run_id"),
          ),
          ignoreError = false
        )
      ),
      postEtlOperations = List.empty,
      dependencies = List.empty
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = support.getOdsBatchTaskName,
      yamlFile = support.getOdsYamlFile,
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "datewarehouse",
          connection = ConnectionEnum.minioParquet.toString,
          configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
        ),
        support.getPostgres,
      ),
      preEtlOperations = List.empty,
      sources = List.apply(
        YamlDataTemplateSource(
          sourceName = "datewarehouse",
          objectName = "src",
          sourceFileSystem = YamlDataTemplateSourceFileSystem(
            tableName = s"stg/${tableSchema}/${sourceTable}",
            tableColumns = List.apply(),
            partitionBy = List.apply("run_id"),
            where = null,
            limit = 0
          ),

          initMode = InitModeEnum.instantly.toString
        )
      ),
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
      ),
      target = List.apply(
        odsSupport.writeOds(
          metadata.columns.isEmpty match {
            case true => WriteMode.overwritePartition
            case false => WriteMode.mergeFull
          }, "level1")
      ),
      postEtlOperations = List.empty,
      dependencies = List.apply(s"stg__batch__${taskName}")
    )

    val clickhouse = new YamlDataEtlToolTemplate(
      taskName = s"clickhouse__batch__${taskName.replace("-", "_")}",
      yamlFile = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__postgres/clickhouse/$sourceTable.yaml",
      connections = List.apply(
        support.getPostgres,
        support.getClickhouseConfig,
      ),
      preEtlOperations =List.apply(support.getClickhouseYamlConfigEltOnServerOperation(metadata)),
      sources =List.apply(support.getDataForDM),
      transform = List.apply(),
      target = List.apply(support.getClickhouseTarget("clickhouseUnico", "src", metadata)      ),
      postEtlOperations =List.apply(support.getClickhouseYamlConfigEltOnServerOperationPost(metadata)),
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


object YamlDataOdsPostgresMirrorTemplate extends YamlClass {
  def apply(inConfig: String, row: Map[String, String]): YamlDataOdsPostgresMirrorTemplate = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsPostgresMirrorTemplate = mapper.readValue(text, classOf[YamlDataOdsPostgresMirrorTemplate])
    return configYaml
  }
}