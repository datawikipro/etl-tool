package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.*
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.yamlConfigSourceKafka.YamlDataTemplateSourceKafkaTopic
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceDBTable, YamlDataTemplateSourceFileSystem, YamlDataTemplateSourceKafka}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetColumn, YamlDataTemplateTargetDatabase, YamlDataTemplateTargetFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.{YamlDataTemplateTransformationExtractSchema, YamlDataTemplateTransformationSparkSql}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport.{YamlDataEtlToolTemplateSupportKafka, YamlDataEtlToolTemplateSupportOds, YamlDataEtlToolTemplateSupportStg}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate}
import pro.datawiki.sparkLoader.connection.databaseTrait.TableMetadataType
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, InitModeEnum, WriteMode}
import pro.datawiki.yamlConfiguration.YamlClass

case class YamlDataOdsDebeziumMongaTableMirror(
                                                taskName: String,
                                                yamlFileCoreLocation: String,
                                                yamlFileLocation: String,
                                                metadataConnection: String,
                                                metadataConfigLocation: String,
                                                kafkaTopic: String,
                                                tableSchema: String,
                                                tableName: String,
                                                dwhConfigLocation: String,
                                                business_date: String,
                                                extra_code: String
                                              ) extends YamlDataTaskToolTemplate {

  override def isRunFromControlDag: Boolean = false

  override def getCoreTask: List[CoreTask] = {
    //val connectionTrait = NoSQLDatabaseTrait.apply(connection, configLocation)
    val metadata = Metadata(metadataConnection, metadataConfigLocation, s"ods__$tableSchema", tableName)
    val kafkaSupport = new YamlDataEtlToolTemplateSupportKafka(
      taskName = taskName,
      kafkaTopic = kafkaTopic,
      yamlFileCoreLocation = yamlFileCoreLocation,
      yamlFileLocation = yamlFileLocation,
      sourceCode = "kafka")

    val stgSupport = new YamlDataEtlToolTemplateSupportStg(
      taskName = taskName,
      sourceLogicTableSchema = tableSchema,
      sourceLogicTableName = tableName,
      yamlFileCoreLocation = yamlFileCoreLocation,
      yamlFileLocation = yamlFileLocation,
      sourceCode = "kafka")

    val odsSupport = new YamlDataEtlToolTemplateSupportOds(
      taskName = taskName,
      tableSchema = tableSchema,
      tableName = tableName,
      metadata = metadata,
      yamlFileCoreLocation = yamlFileCoreLocation,
      yamlFileLocation = yamlFileLocation,
      sourceCode = "kafka")

    val support = new YamlDataEtlToolTemplateSupport(
      taskName = taskName,
      sourceCode = "kafka",
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
    // Create DWH connection to get connection details
    if (dwhConfigLocation == null) {
      throw new IllegalArgumentException("dwhConfigLocation cannot be null")
    }
    val dwhLoader = LoaderPostgres(dwhConfigLocation)
    val dwhInfo = dwhLoader.getDwhConnectionInfo

    val taskNameSync = s"sync__${taskName}"

    val kafka = new YamlDataEtlToolTemplate(
      taskName = kafkaSupport.getKafkaTaskName,
      yamlFile = kafkaSupport.getKafkaYamlFile,
      preEtlOperations = List.empty,
      connections = List(kafkaSupport.getConnect, support.getJsonStreamDataWarehouse),
      sources = List.apply(
        YamlDataTemplateSource(
          sourceName = "kafkaUnico",
          objectName = "source",
          sourceKafka = YamlDataTemplateSourceKafka(
            topics = YamlDataTemplateSourceKafkaTopic(
              topicList = List(kafkaTopic),
            ),
            listTopics = null,
            topicsByRegexp = null
          ),
          initMode = InitModeEnum.instantly.toString
        )
      ),
      transform = List.apply(),
      target = List.apply(
        YamlDataTemplateTarget(
          database = null,
          fileSystem = YamlDataTemplateTargetFileSystem(
            connection = "datewarehouse",
            source = "source",
            mode = WriteMode.streamByRunId,
            targetFile = s"kafka/${kafkaTopic}",
            partitionBy = List.apply("run_id"),
          ),
          messageBroker = null,
          dummy = null,
          ignoreError = false
        )
      ),
      postEtlOperations = List.empty,
      dependencies = List.empty
    )

    val stg = new YamlDataEtlToolTemplate(
      taskName = stgSupport.getStgTaskName,
      yamlFile = stgSupport.getStgYamlFile,
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "datewarehouseJson",
          connection = "minioJson",
          configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
        ),
        YamlDataTemplateConnect(
          sourceName = "datewarehouseParquet",
          connection = ConnectionEnum.minioParquet.toString,
          configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
        ),
      ),
      preEtlOperations = List.empty,
      sources = List.apply(
        YamlDataTemplateSource(
          sourceName = "datewarehouseJson",
          objectName = "source",
          sourceFileSystem = YamlDataTemplateSourceFileSystem(
            tableName = s"kafka/${kafkaTopic}",
            tableColumns = List.apply(),
            partitionBy = List.apply("run_id"),
            where = null,
            limit = 0
          ),
          initMode = InitModeEnum.instantly.toString,
          skipIfEmpty = true
        )
      ),
      transform = List.apply(
        YamlDataTemplateTransformation(
          objectName = "level1",
          cache = null,
          idMap = null,
          sparkSql = null,
          sparkSqlLazy = null,
          extractSchema = YamlDataTemplateTransformationExtractSchema(
            tableName = "source",
            jsonColumn = "value",
            jsonResultColumn = "parsed_value",
            baseSchema = s"/opt/etl-tool/configMigrationSchemas/stg__${kafkaTopic}_value.yaml",
            mergeSchema = true,
          ),
          extractAndValidateDataFrame = null,
          adHoc = null,
          deduplicate = null
        ),
        YamlDataTemplateTransformation(
          objectName = "level2",
          cache = null,
          idMap = null,
          sparkSql = null,
          sparkSqlLazy = null,
          extractSchema = YamlDataTemplateTransformationExtractSchema(
            tableName = "level1",
            jsonColumn = "key",
            jsonResultColumn = "parsed_key",
            baseSchema = s"/opt/etl-tool/configMigrationSchemas/stg__${kafkaTopic}_key.yaml",
            mergeSchema = true
          ),
          extractAndValidateDataFrame = null,
          adHoc = null,
          deduplicate = null
        ),
        YamlDataTemplateTransformation(
          objectName = "level3",
          cache = null,
          idMap = null,
          sparkSql = YamlDataTemplateTransformationSparkSql(
            sql =
              s"""select offset,
                 |       partition,
                 |       timestamp,
                 |       timestampType,
                 |       topic,
                 |       parsed_value as value,
                 |       parsed_key as key
                 |  from level2""".stripMargin
          ),
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = null,
          adHoc = null,
          deduplicate = null
        ),
        YamlDataTemplateTransformation(
          objectName = "level4",
          cache = null,
          idMap = null,
          sparkSql = null,
          sparkSqlLazy = null,
          extractSchema = YamlDataTemplateTransformationExtractSchema(
            tableName = "level3",
            jsonColumn = "value.after",
            jsonResultColumn = "after",
            baseSchema = s"/opt/etl-tool/configMigrationSchemas/stg__${kafkaTopic}_value.after.yaml",
            mergeSchema = true
          ),
          extractAndValidateDataFrame = null,
          adHoc = null,
          deduplicate = null
        ),
      ),
      target = List.apply(
        YamlDataTemplateTarget(
          database = null,
          fileSystem = YamlDataTemplateTargetFileSystem(
            connection = "datewarehouseParquet",
            source = "level4",
            mode = WriteMode.overwritePartition,
            targetFile = s"stg/${kafkaTopic}/kafka",
            partitionBy = List.apply("run_id"),
          ),
          messageBroker = null,
          dummy = null,

          ignoreError = false
        )
      ),
      postEtlOperations = List.empty,
      dependencies = List.apply(kafka.taskName)
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = support.getOdsTaskName,
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
          objectName = "source",
          sourceFileSystem = YamlDataTemplateSourceFileSystem(
            tableName = s"stg/${kafkaTopic}/kafka",
            tableColumns = List.apply(),
            partitionBy = List.apply("run_id"),
            where = null,
            limit = 0
          ),
          skipIfEmpty = true,
          initMode = InitModeEnum.instantly.toString
        )
      ),
      transform = List.apply(
        YamlDataTemplateTransformation(
          objectName = "level1",
          cache = null,
          idMap = null,
          sparkSql = YamlDataTemplateTransformationSparkSql(
            sql =
              s"""select after.*,
                 |       timestamp as ts_ms $extra_code
                 |  from source""".stripMargin,
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
            sql =
              s"""select ${
                metadata.columns.map(col => col.column_name match {
                  case "id" => s"""_id.`$$oid` as ${col.column_name}"""
                  case "user_id" => s"""${col.column_name}.`$$oid` as ${col.column_name}"""
                  case "modified" => s"""${col.column_name}.`$$date` as ${col.column_name}"""
                  case "timestamp" => s"""${col.column_name}.`$$numberLong` as ${col.column_name}"""
                  case _ => s"${col.column_name} as ${col.column_name}"
                }).mkString(",\n      ")
              },ts_ms
                 |  from level1""".stripMargin,
          ),
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = null,
          adHoc = null,
          deduplicate = null
        ),

        YamlDataTemplateTransformation(
          objectName = "level3",
          cache = null,
          idMap = null,
          sparkSql = YamlDataTemplateTransformationSparkSql(
            sql =
              s"""select ${metadata.columns.map(col => s"first_value(${col.column_name}, true) over (partition by ${metadata.primaryKey.mkString(",")} order by ts_ms desc) as ${col.column_name}").mkString(",\n      ")},
                 |       row_number() over (partition by ${metadata.primaryKey.mkString(",")} order by ts_ms desc) as rn
                 |  from level2""".stripMargin,
          ),
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = null,
          adHoc = null,
          deduplicate = null
        ),
        YamlDataTemplateTransformation(
          objectName = "level4",
          cache = null,
          idMap = null,
          sparkSql = YamlDataTemplateTransformationSparkSql(
            sql =
              s"""select ${metadata.columns.map(col => s"${col.column_name}").mkString(",\n      ")}
                 |  from level3
                 | where rn = 1""".stripMargin,
          ),
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = null,
          adHoc = null,
          deduplicate = null
        ),
      ),
      target = List.apply(
        YamlDataTemplateTarget(
          fileSystem = null,
          messageBroker = null,
          dummy = null,

          database = YamlDataTemplateTargetDatabase(
            connection = "postgres",
            source = "level4",
            mode = WriteMode.mergeDelta,
            partitionMode = null, //TODO
            targetSchema = s"ods__${tableSchema}",
            targetTable = s"${tableName}",
            columns = metadata.columns.map(col =>
              YamlDataTemplateTargetColumn(
                columnName = col.column_name,
                isNullable = true,
                columnType = col.data_type.getTypeInSystem("postgres"),
                columnTypeDecode = col.data_type match {
                  case TableMetadataType.TimestampWithoutTimeZone => true
                  case _ => false
                }
              )),
            uniqueKey = metadata.primaryKey,
            partitionBy = null,
            scd = "SCD_3"
          ),
          ignoreError = false
        )
      ),
      postEtlOperations = List.empty,
      dependencies = List.apply(stg.taskName)
    )

    val clickhouse = new YamlDataEtlToolTemplate(
      taskName = support.getClickhouseTaskName,
      yamlFile =support.getClickhouseYamlFile,
      connections = List.apply(
        support.getPostgres,support.getClickhouseConfig),
      preEtlOperations = List.apply(support.getClickhouseYamlConfigEltOnServerOperation(metadata)),
      sources = List.apply(support.getDataForDM),
      transform = List.apply(),
      target = List.apply(support.getClickhouseTarget("clickhouseUnico", "src", metadata)      ),
      postEtlOperations =List.apply(support.getClickhouseYamlConfigEltOnServerOperationPost(metadata)),
      dependencies = List.apply(ods.taskName)
    )

    val snowflake = new YamlDataEtlToolTemplate(
      taskName = support.getSnowflakeTaskName,
      yamlFile = support.getSnowflakeYamlFile,
      connections = List.apply(support.getPostgres, support.getAmazonS3),
      preEtlOperations = List.empty,
      sources = List.apply(support.getDataForDM),
      transform = List.apply(),
      target = List.apply(support.getSnowflakeTarget("datewarehouse", "src")),
      postEtlOperations = List.empty,
      dependencies = List.apply(ods.taskName)
    )
    return kafka.getCoreTask ++ stg.getCoreTask ++ ods.getCoreTask ++ clickhouse.getCoreTask ++ snowflake.getCoreTask
  }

}


object YamlDataOdsDebeziumMongaTableMirror extends YamlClass {
  def apply(inConfig: String, row: Map[String, String]): YamlDataOdsDebeziumMongaTableMirror = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsDebeziumMongaTableMirror = mapper.readValue(text, classOf[YamlDataOdsDebeziumMongaTableMirror])
    return configYaml
  }
}