package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.*
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetDummy, YamlDataTemplateTargetFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.{YamlDataTemplateTransformationExtractSchema, YamlDataTemplateTransformationSparkSql}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport.{YamlDataEtlToolTemplateSupportKafka, YamlDataEtlToolTemplateSupportOds, YamlDataEtlToolTemplateSupportStg}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate}
import pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation.YamlConfigEltOnServerSQL
import pro.datawiki.sparkLoader.connection.clickhouse.LoaderClickHouse
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode
import pro.datawiki.yamlConfiguration.YamlClass

case class YamlDataOdsKafkaTemplate(
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
    val metadata = Metadata(metadataConnection, metadataConfigLocation, s"ods__$tableSchema", tableName)

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

    if (dwhConfigLocation == null) {
      throw new IllegalArgumentException("dwhConfigLocation cannot be null")
    }
    val dwhLoader = LoaderPostgres(dwhConfigLocation)
    val dwhInfo = dwhLoader.getDwhConnectionInfo

    val kafka = new YamlDataEtlToolTemplate(
      taskName = kafkaSupport.getKafkaTaskName,
      yamlFile = kafkaSupport.getKafkaYamlFile,
      preEtlOperations = List.empty,
      connections = List(kafkaSupport.getConnect, support.getJsonStreamDataWarehouse),
      sources = List.apply(kafkaSupport.getSource),
      transform = List.empty,
      target = List(kafkaSupport.getTarget(support.getJsonStreamDataWarehouse.getSourceName)),
      postEtlOperations = List.empty,
      dependencies = List.empty
    )

    val stg = new YamlDataEtlToolTemplate(
      taskName = stgSupport.getStgTaskName,
      yamlFile = stgSupport.getStgYamlFile,
      connections = List(
        support.getJsonDataWarehouse,
        support.getParquetDataWarehouse,
      ),
      preEtlOperations = List.empty,
      sources = List(kafkaSupport.getReadTarget(support.getJsonDataWarehouse.getSourceName)),
      transform = List(
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
            baseSchema = s"/opt/etl-tool/configMigrationSchemas/stg__$kafkaTopic.yaml",
            mergeSchema = false,
          ),
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
              s"""select offset,
                 |       partition,
                 |       timestamp,
                 |       timestampType,
                 |       topic,
                 |       ${business_date} as business_date,
                 |       parsed_value as value
                 |  from level1""".stripMargin
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
              s"""select *
                 |  from level2""".stripMargin
          ),
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = null,
          adHoc = null,
          deduplicate = null
        ),
      ),
      target = List(
        YamlDataTemplateTarget(
          database = null,
          fileSystem = YamlDataTemplateTargetFileSystem(
            connection = support.getParquetDataWarehouse.getSourceName,
            source = "level4",
            mode = WriteMode.autoOverwrite,
            targetFile = stgSupport.getStgFolder,
            partitionBy = List("business_date"),
          ),
          messageBroker = null,
          dummy = null,

          ignoreError = false
        )
      ),
      postEtlOperations = List.empty,
      dependencies = List(kafka.taskName)
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = support.getOdsTaskName,
      yamlFile = support.getOdsYamlFile,
      connections = List.apply(support.getParquetDataWarehouse, support.getPostgres),
      preEtlOperations = List.empty,
      sources = List.apply(stgSupport.getOdsYamlDataTemplateSourceYamlDataTemplateSource(support.getParquetDataWarehouse.getSourceName)),
      transform = List.apply(
        YamlDataTemplateTransformation(
          objectName = "level1",
          cache = null,
          idMap = null,
          sparkSql = YamlDataTemplateTransformationSparkSql(
            sql =
              s"""select `offset` as offset_id,
                 |       value.*,
                 |       timestamp as ts_ms ${extra_code}
                 |  from src""".stripMargin,
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
              s"""select ${metadata.columns.map(col => col.column_name).mkString(",\n      ")}, ts_ms
                 |  from level1""".stripMargin,
          ),
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = null,
          adHoc = null,
          deduplicate = null
        ),
      ),
      target = List.apply(odsSupport.writeOds(WriteMode.append, "level2")),
      postEtlOperations = List.empty,
      dependencies = List.apply(stg.taskName)
    )
    val password = "H76q3kng6tsfdpoi90$&97oj013"
    val clickhouse = new YamlDataEtlToolTemplate(
      taskName = support.getClickhouseTaskName,
      yamlFile = support.getClickhouseYamlFile,
      connections = List.apply(
        support.getPostgres,
        support.getClickhouseConfig,
      ),
      preEtlOperations = List.apply(support.getClickhouseYamlConfigEltOnServerOperation(metadata)),
      sources =List.apply(support.getDataForDM),
      transform = List.apply(),
      target = List.apply(support.getClickhouseTarget("clickhouseUnico", "src", metadata)      ),
      postEtlOperations =List.apply(support.getClickhouseYamlConfigEltOnServerOperationPost(metadata)),
      dependencies = List.apply(ods.taskName)
    )

    val snowflake = new YamlDataEtlToolTemplate(
      taskName = support.getSnowflakeTaskName,
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

    return kafka.getCoreTask ++ stg.getCoreTask ++ ods.getCoreTask ++ clickhouse.getCoreTask ++ snowflake.getCoreTask
  }

}


object YamlDataOdsKafkaTemplate extends YamlClass {
  def apply(inConfig: String, row: Map[String, String]): YamlDataOdsKafkaTemplate = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsKafkaTemplate = mapper.readValue(text, classOf[YamlDataOdsKafkaTemplate])
    return configYaml
  }
}