package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.*
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.yamlConfigSourceKafka.YamlDataTemplateSourceKafkaTopic
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceFileSystem, YamlDataTemplateSourceKafka}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetColumn, YamlDataTemplateTargetDatabase, YamlDataTemplateTargetDummy, YamlDataTemplateTargetFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.{YamlDataTemplateTransformationExtractSchema, YamlDataTemplateTransformationSparkSql}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate}
import pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation.YamlConfigEltOnServerSQL
import pro.datawiki.sparkLoader.connection.clickhouse.LoaderClickHouse
import pro.datawiki.sparkLoader.connection.databaseTrait.TableMetadataType
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, InitModeEnum, PartitionModeEnum, WriteMode}
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

case class YamlDataOdsKafkaTemplate(
                                     taskName: String=throw Exception(),
                                     yamlFileCoreLocation: String=throw Exception(),
                                     yamlFileLocation: String=throw Exception(),
                                     metadataConnection: String=throw Exception(),
                                     metadataConfigLocation: String=throw Exception(),
                                     kafkaTopic: String=throw Exception(),
                                     tableSchema: String=throw Exception(),
                                     tableName: String=throw Exception(),
                                     dwhConfigLocation: String =throw Exception()
                                   ) extends YamlDataTaskToolTemplate {
  override def getCoreTask: List[CoreTask] = {
    //        val connectionTrait = NoSQLDatabaseTrait.apply(connection, configLocation)

    val metadata = Metadata(metadataConnection, metadataConfigLocation, s"ods__$tableSchema", tableName)
    if (dwhConfigLocation == null) {
      throw new IllegalArgumentException("dwhConfigLocation cannot be null")
    }
    val dwhLoader = LoaderPostgres(dwhConfigLocation)
    val dwhInfo = dwhLoader.getDwhConnectionInfo

    val kafka = new YamlDataEtlToolTemplate(
      taskName = s"kafka__${taskName}",
      yamlFile = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__kafka/kafka/$kafkaTopic.yaml",
      preEtlOperations = List.empty,
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "kafkaUnico",
          connection = "kafkaSaslSSL",
          configLocation = "/opt/etl-tool/configConnection/kafka.yaml"
        ),
        YamlDataTemplateConnect(
          sourceName = "datewarehouse",
          connection = "minioJsonStream",
          configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
        ),
      ),
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
            partitionMode = PartitionModeEnum.streamByRunId.toString,
            targetFile = s"kafka/${kafkaTopic}",
            partitionBy = List.apply(),
          ),
          messageBroker = null,
          dummy = null,
          ignoreError = false
        )
      ),
      dependencies = List.empty
    )

    val stg = new YamlDataEtlToolTemplate(
      taskName = s"stg__${taskName}",
      yamlFile = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__kafka/stg/$kafkaTopic.yaml",
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
            baseSchema = s"/opt/etl-tool/configMigrationSchemas/stg__${kafkaTopic}.yaml", //TODO
          ),
          extractAndValidateDataFrame = null,
          adHoc = null
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
                 |       parsed_value as value
                 |  from level1""".stripMargin
          ),
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = null,
          adHoc = null
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
          adHoc = null
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
            partitionMode = PartitionModeEnum.direct.toString,
          ),
          messageBroker = null,
          dummy = null,

          ignoreError = false
        )
      ),
      dependencies = List.apply(kafka.taskName)
    )
    val stg_reload = new YamlDataEtlToolTemplate(
      taskName = s"stg_reload__${taskName}",
      yamlFile = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__kafka/stg_reload/$kafkaTopic.yaml",
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
            baseSchema = s"/opt/etl-tool/configMigrationSchemas/stg__${kafkaTopic}.yaml", //TODO
          ),
          extractAndValidateDataFrame = null,
          adHoc = null
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
                 |       parsed_value as value
                 |  from level1""".stripMargin
          ),
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = null,
          adHoc = null
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
          adHoc = null
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
            partitionMode = PartitionModeEnum.direct.toString,
          ),
          messageBroker = null,
          dummy = null,

          ignoreError = false
        )
      ),
      dependencies = List.apply("branching")
    )
    val ods = new YamlDataEtlToolTemplate(
      taskName = s"ods__${taskName}",
      yamlFile = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__kafka/ods/$kafkaTopic.yaml",
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "datewarehouse",
          connection = ConnectionEnum.minioParquet.toString,
          configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
        ),
        YamlDataTemplateConnect(
          sourceName = "postgres",
          connection = "postgres",
          configLocation = "/opt/etl-tool/configConnection/postgres.yaml"
        ),
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
              s"""select `offset` as offset_id,
                 |       value.*,
                 |       timestamp as ts_ms
                 |  from source""".stripMargin,
          ),
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = null,
          adHoc = null
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
          adHoc = null
        ),
      ),
      target = List.apply(
        YamlDataTemplateTarget(
          fileSystem = null,
          messageBroker = null,
          dummy = null,

          database = YamlDataTemplateTargetDatabase(
            connection = "postgres",
            source = "level2",
            mode = WriteMode.merge,
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
            deduplicationKey = List.apply("ts_ms desc"),
            partitionBy = null
          ),
          ignoreError = false
        )
      ),
      dependencies = List.apply(stg.taskName)
    )
    val password = "H76q3kng6tsfdpoi90$&97oj013"
    val clickhouse = new YamlDataEtlToolTemplate(
      taskName = s"clickhouse__${taskName}",
      yamlFile = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__kafka/clickhouse/$kafkaTopic.yaml",
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "clickhouseUnico",
          connection = "clickhouse",
          configLocation = "/opt/etl-tool/configConnection/clickhouse.yaml"
        ),
      ),
      preEtlOperations = List.apply(
        YamlConfigEltOnServerOperation(
          eltOnServerOperationName = "preSql",
          sourceName = "clickhouseUnico",
          sql = YamlConfigEltOnServerSQL(
            sql = List.apply(
              s"""CREATE TABLE IF NOT EXISTS ods__${tableSchema}._${tableName}
                 |(
                 |    ${metadata.columns.map(col => s"${col.column_name} ${LoaderClickHouse.encodeIsNullable(col.isNullable, LoaderClickHouse.encodeDataType(col.data_type))}").mkString(",\n    ")}, valid_from_dttm Datetime, valid_to_dttm Datetime
                 |)
                 |    ENGINE = PostgreSQL('${dwhInfo.hostPort}', '${dwhInfo.database}', '${tableName}', '${dwhInfo.username}', '${dwhInfo.password}', 'ods__${tableSchema}', 'connect_timeout=15, read_write_timeout=300');""".stripMargin,
              s"""drop table if exists ods__${tableSchema}.${tableName}_new;""",
              s"""CREATE TABLE IF NOT EXISTS ods__${tableSchema}.${tableName}_new
                 |(
                 |    ${metadata.columns.map(col => s"${col.column_name} ${LoaderClickHouse.encodeIsNullable(col.isNullable, LoaderClickHouse.encodeDataType(col.data_type))}").mkString(",\n    ")}, valid_from_dttm Datetime, valid_to_dttm Datetime
                 |)
                 |    ENGINE = MergeTree ORDER BY (${metadata.primaryKey.mkString(",")})  SETTINGS index_granularity = 8192;""".stripMargin,
              s"""insert into ods__${tableSchema}.${tableName}_new (${metadata.columns.map(col => s"${col.column_name}").mkString(",")}, valid_from_dttm , valid_to_dttm )
                 |select ${metadata.columns.map(col => s"${col.column_name}").mkString(",")}, valid_from_dttm , valid_to_dttm
                 |  from ods__${tableSchema}._${tableName}
                 | where valid_to_dttm = cast('2100-01-01' as Date)
                 | SETTINGS external_storage_connect_timeout_sec=3000000,external_storage_rw_timeout_sec=3000000,connect_timeout=3000000;""".stripMargin,
              s"""drop table if exists ods__${tableSchema}.${tableName}; """,
              s"""rename table ods__${tableSchema}.${tableName}_new to ods__${tableSchema}.${tableName};"""

            ),
          ),
          ignoreError = false
        )
      ),
      sources = List.apply(),
      transform = List.apply(),
      target = List.apply(
        YamlDataTemplateTarget(
          database = null,
          fileSystem = null,
          messageBroker = null,
          dummy = YamlDataTemplateTargetDummy(),
          ignoreError = false
        )
      ),
      dependencies = List.apply(ods.taskName)
    )

    return kafka.getCoreTask ++ stg.getCoreTask ++ ods.getCoreTask ++ clickhouse.getCoreTask
  }

}


object YamlDataOdsKafkaTemplate extends YamlClass {
  def apply(inConfig: String, row: mutable.Map[String, String]): YamlDataOdsKafkaTemplate = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsKafkaTemplate = mapper.readValue(text, classOf[YamlDataOdsKafkaTemplate])
    return configYaml
  }
}