package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.*
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.YamlDataTemplateTargetFileSystem
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.{YamlDataTemplateTransformationExtractSchema, YamlDataTemplateTransformationSparkSql}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport.{YamlDataEtlToolTemplateSupportKafka, YamlDataEtlToolTemplateSupportOds, YamlDataEtlToolTemplateSupportStg}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate}
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
                                     extra_code: String,
                                     stgSchemaPath: String // Added this field
                                   ) extends YamlDataTaskToolTemplate {

  override def isRunFromControlDag: Boolean = false

  override def getCoreTask: List[CoreTask] = {
    val kafkaSupport = new YamlDataEtlToolTemplateSupportKafka(
      taskName = taskName,
      kafkaTopic = kafkaTopic,
      yamlFileCoreLocation = yamlFileCoreLocation,
      yamlFileLocation = yamlFileLocation,
      sourceCode = "kafka",
      sourceLogicTableSchema = tableSchema)

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
      yamlFileLocation = yamlFileLocation)

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

    if dwhConfigLocation == null then {
      throw new IllegalArgumentException("dwhConfigLocation cannot be null")
    }
    val dwhLoader = LoaderPostgres(dwhConfigLocation)
    val dwhInfo = dwhLoader.getDwhConnectionInfo

    val kafka = new YamlDataEtlToolTemplate(
      taskName = kafkaSupport.getKafkaTaskName,
      yamlFile = kafkaSupport.getKafkaYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(kafkaSupport.getSource(kafkaSupport.getConnect)),
      transform = List.empty,
      target = List(kafkaSupport.getTarget(support.getJsonStreamDataWarehouse, List.empty)),
      postEtlOperations = List.empty,
      dependencies = List.empty
    )

    val stg = new YamlDataEtlToolTemplate(
      taskName = stgSupport.getStgTaskName,
      yamlFile = stgSupport.getStgYamlFile,
      preEtlOperations = List.empty,
      sources = List(kafkaSupport.getReadTarget(support.getJsonDataWarehouse)),
      transform = List(
        stgSupport.getExtractSchema("level1", "source", "value", "parsed_value", kafkaTopic),
        support.getSparkSqlTransformation("level2",
          s"""select offset,
             |       partition,
             |       timestamp,
             |       timestampType,
             |       topic,
             |       ${business_date} as business_date,
             |       parsed_value as value
             |  from level1""".stripMargin),
        support.getSparkSqlTransformation("level4",
          s"""select *
             |  from level2""".stripMargin),
      ),
      target = List(
        stgSupport.writeStg(WriteMode.autoOverwrite, "level4", support.getParquetDataWarehouse)
      ),
      postEtlOperations = List.empty,
      dependencies = List(kafka.getTaskName)
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = support.getOdsTaskName,
      yamlFile = support.getOdsYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(stgSupport.getOdsYamlDataTemplateSource(support.getParquetDataWarehouse)),
      transform = List.apply(
        support.getSparkSqlTransformation("level1",
          s"""select `offset` as offset_id,
             |       value.*,
             |       timestamp as ts_ms ${extra_code}
             |  from src""".stripMargin),
        support.getSparkSqlTransformation("level2",
          s"""select ${metadata.columns.map(col => col.column_name).mkString(",\n      ")}, ts_ms
             |  from level1""".stripMargin)
      ),
      target = List.apply(odsSupport.writeOds(support.getPostgres,WriteMode.append, "level2")),
      postEtlOperations = List.empty,
      dependencies = List.apply(stg.getTaskName)
    )

    val clickhouse = new YamlDataEtlToolTemplate(
      taskName = support.getClickhouseTaskName,
      yamlFile = support.getClickhouseYamlFile,
      preEtlOperations = List.apply(support.getClickhouseYamlConfigEltOnServerOperation(metadata)),
      sources = List.apply(support.getDataForDM(support.getPostgres)),
      transform = List.empty,
      target = List.apply(support.getClickhouseTarget(support.getClickhouseConfig, "src", metadata)),
      postEtlOperations = List.apply(support.getClickhouseYamlConfigEltOnServerOperationPost(metadata)),
      dependencies = List.apply(ods.getTaskName)
    )

    val snowflake = new YamlDataEtlToolTemplate(
      taskName = support.getSnowflakeTaskName,
      yamlFile = support.getSnowflakeYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(support.getDataForDM(support.getPostgres)),
      transform = List.empty,
      target = List.apply(support.getSnowflakeTarget(support.getAmazonS3, "src")),
      postEtlOperations = List.empty,
      dependencies = List.apply(ods.getTaskName)
    )

    return kafka.getCoreTask ++ stg.getCoreTask ++ ods.getCoreTask ++ clickhouse.getCoreTask ++ snowflake.getCoreTask
  }

}


object YamlDataOdsKafkaTemplate extends YamlClass {
  /**
   * This apply method reads a YAML configuration and maps it to a YamlDataOdsKafkaTemplate case class.
   * Ensure that the YAML configuration includes the 'stgSchemaPath' field, as it is now a required parameter.
   */
  def apply(inConfig: String, row: Map[String, String]): YamlDataOdsKafkaTemplate = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsKafkaTemplate = mapper.readValue(text, classOf[YamlDataOdsKafkaTemplate])
    return configYaml
  }
}