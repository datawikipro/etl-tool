package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.*
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport.{YamlDataEtlToolTemplateSupportKafka, YamlDataEtlToolTemplateSupportOds, YamlDataEtlToolTemplateSupportStg}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate}
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode
import pro.datawiki.yamlConfiguration.YamlClass

case class YamlDataOdsDebeziumPostgresTableMirror(
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
    val metadata = Metadata(metadataConnection, metadataConfigLocation, s"ods__$tableSchema", tableName)
    val kafkaSupport = new YamlDataEtlToolTemplateSupportKafka(
      taskName = taskName,
      kafkaTopic = kafkaTopic,
      yamlFileCoreLocation = yamlFileCoreLocation,
      yamlFileLocation = yamlFileLocation,
      sourceCode = "debezium",
      sourceLogicTableSchema = tableSchema)

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
      yamlFileLocation = yamlFileLocation)

    val support = new YamlDataEtlToolTemplateSupport(
      taskName = taskName,
      sourceCode = "debezium",
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

    val taskNameStg = s"stg__${taskName}"
    val taskNameSync = s"sync__${taskName}"

    val kafka = new YamlDataEtlToolTemplate(
      taskName = kafkaSupport.getKafkaTaskName,
      yamlFile = kafkaSupport.getKafkaYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(kafkaSupport.getKafkaSource(kafkaSupport.getConnect)),
      transform = List.empty,
      target = List.apply(kafkaSupport.getTarget(support.getJsonStreamDataWarehouse, List.apply("locationBasedOnRunId"))),
      postEtlOperations = List.empty,
      dependencies = List.empty
    )

    val stg = new YamlDataEtlToolTemplate(
      taskName = taskNameStg,
      yamlFile = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__debezium/stg/$kafkaTopic.yaml",
      preEtlOperations = List.empty,
      sources = List.apply(support.getSrcFromS3(support.getJsonDataWarehouse, "source", s"kafka/$kafkaTopic")),
      transform = List.apply(
        stgSupport.getExtractSchema("level1", "source", "value", "parsed_value", kafkaTopic),
        stgSupport.getExtractSchema("level2", "level1", "key", "parsed_key", kafkaTopic),
        support.getSparkSqlTransformation("level3",
          s"""select offset,
             |       partition,
             |       timestamp,
             |       timestampType,
             |       topic,
             |       parsed_value as value,
             |       parsed_key as key
             |  from level2""".stripMargin),
        support.getSparkSqlTransformation("level4", s"""select * from level3""".stripMargin),
      ),
      target = List.apply(stgSupport.getStgYamlDataTemplateTarget(support.getParquetDataWarehouse, "level4", List.apply("run_id"))
      ),
      postEtlOperations = List.empty,
      dependencies = List.apply(kafka.getTaskName)
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = support.getOdsTaskName,
      yamlFile = support.getOdsYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(support.getSrcFromS3(support.getParquetDataWarehouse, "source", s"kafka/${kafkaTopic}/kafka")),
      transform = List.apply(
        support.getSparkSqlTransformation(
          "level1",
          s"""select value.after.*,
             |       timestamp as ts_ms $extra_code
             |  from source""".stripMargin),
        support.getSparkSqlTransformation(
          "level2",
          s"""select ${metadata.columns.map(col => col.column_name).mkString(",\n      ")}, ts_ms
             |  from level1""".stripMargin),
        support.getDeduplicate("level3", "level2", metadata.primaryKey, List.apply("ts_ms")),
      ),
      target = List.apply(odsSupport.writeOds(support.getPostgres,WriteMode.mergeDelta, "level3")),
      postEtlOperations = List.empty,
      dependencies = List.apply(taskNameStg)
    )

    val clickhouse = new YamlDataEtlToolTemplate(
      taskName = support.getClickhouseTaskName,
      yamlFile = support.getClickhouseYamlFile,
      preEtlOperations = List.apply(support.getClickhouseYamlConfigEltOnServerOperation(metadata)),
      sources = List.apply(support.getDataForDM(support.getPostgres)),
      transform = List.empty,
      target = List.apply(support.getClickhouseTarget( support.getClickhouseConfig, "src", metadata)),
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


object YamlDataOdsDebeziumPostgresTableMirror extends YamlClass {
  def apply(inConfig: String, row: Map[String, String]): YamlDataOdsDebeziumPostgresTableMirror = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsDebeziumPostgresTableMirror = mapper.readValue(text, classOf[YamlDataOdsDebeziumPostgresTableMirror])
    return configYaml
  }
}