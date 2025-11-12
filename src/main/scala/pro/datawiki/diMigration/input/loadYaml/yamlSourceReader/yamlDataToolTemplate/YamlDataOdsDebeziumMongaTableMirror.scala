package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.*
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport.{YamlDataEtlToolTemplateSupportKafka, YamlDataEtlToolTemplateSupportOds, YamlDataEtlToolTemplateSupportStg}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate}
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode
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
      sourceCode = "debezium")

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
    // Create DWH connection to get connection details
    if dwhConfigLocation == null then {
      throw new IllegalArgumentException("dwhConfigLocation cannot be null")
    }
    val dwhLoader = LoaderPostgres(dwhConfigLocation)
    val dwhInfo = dwhLoader.getDwhConnectionInfo

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
      taskName = stgSupport.getStgTaskName,
      yamlFile = stgSupport.getStgYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(support.getSrcFromS3(support.getJsonDataWarehouse, "source", s"kafka/${kafkaTopic}")),
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
        stgSupport.getExtractSchema("level4", "level3", "value.after", "after", kafkaTopic),
      ),
      target = List.apply(stgSupport.getStgYamlDataTemplateTarget(support.getParquetDataWarehouse, "level4", List.apply("run_id"))      ),
      postEtlOperations = List.empty,
      dependencies = List.apply(kafka.getTaskName)
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = support.getOdsTaskName,
      yamlFile = support.getOdsYamlFile,
      preEtlOperations = List.empty,
      sources = List.apply(stgSupport.getKafkaYamlDataTemplateSource(support.getParquetDataWarehouse, s"stg/${kafkaTopic}/kafka")),
      transform = List.apply(
        support.getSparkSqlTransformation("level1",
          s"""select after.*,
             |       timestamp as ts_ms $extra_code
             |  from source""".stripMargin),
        support.getSparkSqlTransformation("level2",
          s"""select ${
            metadata.columns.map(col => col.column_name match {
              case "id" => s"""_id.`$$oid` as ${col.column_name}"""
              case "user_id" => s"""${col.column_name}.`$$oid` as ${col.column_name}"""
              case "modified" => s"""${col.column_name}.`$$date` as ${col.column_name}"""
              case "timestamp" => s"""${col.column_name}.`$$numberLong` as ${col.column_name}"""
              case _ => s"${col.column_name} as ${col.column_name}"
            }).mkString(",\n      ")
          },ts_ms
             |  from level1""".stripMargin),

        support.getSparkSqlTransformation("level3",
          s"""select ${metadata.columns.map(col => s"first_value(${col.column_name}, true) over (partition by ${metadata.primaryKey.mkString(",")} order by ts_ms desc) as ${col.column_name}").mkString(",\n      ")},
             |       row_number() over (partition by ${metadata.primaryKey.mkString(",")} order by ts_ms desc) as rn
             |  from level2""".stripMargin),
        support.getSparkSqlTransformation("level4",
          s"""select ${metadata.columns.map(col => s"${col.column_name}").mkString(",\n      ")}
             |  from level3
             | where rn = 1""".stripMargin),
      ),
      target = List.apply(odsSupport.writeOds(support.getPostgres,WriteMode.mergeDelta, "level4")),
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


object YamlDataOdsDebeziumMongaTableMirror extends YamlClass {
  def apply(inConfig: String, row: Map[String, String]): YamlDataOdsDebeziumMongaTableMirror = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsDebeziumMongaTableMirror = mapper.readValue(text, classOf[YamlDataOdsDebeziumMongaTableMirror])
    return configYaml
  }
}