package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceBigQuery, YamlDataTemplateSourceDBTable, YamlDataTemplateSourceFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetColumn, YamlDataTemplateTargetDatabase, YamlDataTemplateTargetFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.YamlDataTemplateTransformationExtractAndValidateDataFrame
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.{YamlDataTemplateConnect, YamlDataTemplateSource, YamlDataTemplateTarget, YamlDataTemplateTransformation}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.YamlConfigEltOnServerOperation
import pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation.YamlConfigEltOnServerSQL
import pro.datawiki.sparkLoader.connection.clickhouse.LoaderClickHouse
import pro.datawiki.sparkLoader.connection.databaseTrait.TableMetadata
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, InitModeEnum, PartitionModeEnum, WriteMode}


class YamlDataEtlToolTemplateSupport(
                                      taskName: String = throw Exception(),
                                      sourceTableSchema: String = throw Exception(),
                                      sourceTableName: String = throw Exception(),
                                      sourceLogicTableSchema: String = throw Exception(),
                                      sourceLogicTableName: String = throw Exception(),
                                      targetTableSchema: String = throw Exception(),
                                      targetTableName: String = throw Exception(),
                                      connection: String = throw Exception(),
                                      yamlFileCoreLocation: String = throw Exception(),
                                      yamlFileLocation: String = throw Exception()
                                    ) {

  val dwhConfigLocation = "/opt/etl-tool/configConnection/postgres.yaml"
  val dwhLoader = LoaderPostgres(dwhConfigLocation)
  val dwhInfo = dwhLoader.getDwhConnectionInfo

  def getStgTaskName: String = s"stg__batch__${taskName}"

  def getOdsTaskName: String = s"ods__batch__${taskName}"

  def getClickhouseTaskName: String = s"clickhouse__batch__${taskName}"

  def getStgFolder: String = s"stg/${sourceLogicTableSchema}/${sourceLogicTableName}"

  def getStgYamlFile: String = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__bigQuery/stg/$taskName.yaml"

  def getOdsYamlFile: String = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__bigQuery/ods/$taskName.yaml"

  def getClickhouseYamlFile: String = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__bigQuery/Clickhouse/$taskName.yaml"

  def getParquetDataWarehouse: YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = "datewarehouse",
    connection = ConnectionEnum.minioParquet.toString,
    configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
  )

  def getMainDataWarehouseName: String = "datewarehouseMain"

  def getMainDataWarehouse: YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = getMainDataWarehouseName,
    connection = ConnectionEnum.postgres.toString,
    configLocation = dwhConfigLocation
  )

  def getClickhouseDataWarehouse: YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = "clickhouseUnico",
    connection = "clickhouse",
    configLocation = "/opt/etl-tool/configConnection/clickhouse.yaml"
  )

  def getYamlDataTemplateTransformation: YamlDataTemplateTransformation =
    YamlDataTemplateTransformation(
      objectName = "schema",
      cache = null,
      idMap = null,
      sparkSql = null,
      sparkSqlLazy = null,
      extractSchema = null,
      extractAndValidateDataFrame = YamlDataTemplateTransformationExtractAndValidateDataFrame(
        dataFrameIn = "src",
        configLocation = s"/opt/etl-tool/configMigrationSchemas/${connection}__${sourceLogicTableSchema}__$sourceLogicTableName.yaml"
      ),
      adHoc = null,
      deduplicate = null
    )

  def getStgYamlDataTemplateTarget: YamlDataTemplateTarget =
    YamlDataTemplateTarget(
      database = null,
      messageBroker = null,
      dummy = null,
      fileSystem = YamlDataTemplateTargetFileSystem(
        connection = "datewarehouse",
        source = "src",
        mode = WriteMode.overwritePartition,
        partitionMode = PartitionModeEnum.direct.toString,
        targetFile = getStgFolder,
        partitionBy = List.apply("run_id"),
      ),
      ignoreError = false
    )

  def getOdsYamlDataTemplateSourceYamlDataTemplateSource: YamlDataTemplateSource = YamlDataTemplateSource(
    sourceName = "datewarehouse",
    objectName = "src",
    sourceFileSystem = YamlDataTemplateSourceFileSystem(
      tableName = getStgFolder,
      tableColumns = List.apply(),
      partitionBy = List.apply("run_id"),
      where = null,
      limit = 0
    ),
    initMode = InitModeEnum.instantly.toString
  )

  def getOdsYamlDataTemplateTarget(metadata: TableMetadata): YamlDataTemplateTarget =
    YamlDataTemplateTarget(
      database = YamlDataTemplateTargetDatabase(
        connection = getMainDataWarehouseName,
        source = "src",
        mode = metadata.columns.isEmpty match {
          case true => WriteMode.overwritePartition
          case false => WriteMode.merge
        },
        partitionMode = null, //TODO
        targetSchema = s"ods__${targetTableSchema}",
        targetTable = s"${targetTableName}",
        columns = metadata.columns.map(col =>
          YamlDataTemplateTargetColumn(
            columnName = col.column_name,
            isNullable = true,
            columnType = col.data_type.getTypeInSystem("postgres"),
            columnTypeDecode = false
          )),
        uniqueKey = metadata.primaryKey,
        deduplicationKey = List.apply(),
        partitionBy = null,
        scd = "SCD_3"

      ),
      fileSystem = null,
      messageBroker = null,
      dummy = null,
      ignoreError = false
    )

  def getClickhouseYamlConfigEltOnServerOperation(metadata: TableMetadata): YamlConfigEltOnServerOperation =
    YamlConfigEltOnServerOperation(
      eltOnServerOperationName = "preSql",
      sourceName = "clickhouseUnico",
      sql = YamlConfigEltOnServerSQL(
        sql = List.apply(
          s"""CREATE TABLE IF NOT EXISTS ods__${targetTableSchema}._${targetTableName}
             |(
             |    ${metadata.columns.map(col => s"${col.column_name} ${LoaderClickHouse.encodeIsNullable(col.isNullable, LoaderClickHouse.encodeDataType(col.data_type))}").mkString(",\n    ")}, valid_from_dttm Datetime, valid_to_dttm Datetime
             |)
             |    ENGINE = PostgreSQL('${dwhInfo.hostPort}', '${dwhInfo.database}', '${targetTableName}', '${dwhInfo.username}', '${dwhInfo.password}', 'ods__${targetTableSchema}', 'connect_timeout=15, read_write_timeout=300');""".stripMargin,
          s"""drop table if exists ods__${targetTableSchema}.${targetTableName}_new;""",
          s"""CREATE TABLE IF NOT EXISTS ods__${targetTableSchema}.${targetTableName}_new
             |(
             |    ${metadata.columns.map(col => s"${col.column_name} ${LoaderClickHouse.encodeIsNullable(col.isNullable, LoaderClickHouse.encodeDataType(col.data_type))}").mkString(",\n    ")}, valid_from_dttm Datetime, valid_to_dttm Datetime
             |)
             |    ENGINE = MergeTree ORDER BY (${metadata.primaryKey.mkString(",")})  SETTINGS index_granularity = 8192;""".stripMargin,
          s"""insert into ods__${targetTableSchema}.${targetTableName}_new (${metadata.columns.map(col => s"${col.column_name}").mkString(",")}, valid_from_dttm , valid_to_dttm )
             |select ${metadata.columns.map(col => s"${col.column_name}").mkString(",")}, valid_from_dttm , valid_to_dttm
             |  from ods__${targetTableSchema}._${targetTableName}
             | where valid_to_dttm = cast('2100-01-01' as Date)
             | SETTINGS external_storage_connect_timeout_sec=3000000,external_storage_rw_timeout_sec=3000000,connect_timeout=3000000;""".stripMargin,
          s"""drop table if exists ods__${targetTableSchema}.${targetTableName}; """,
          s"""rename table ods__${targetTableSchema}.${targetTableName}_new to ods__${targetTableSchema}.${targetTableName};"""
        ),
      ),
      ignoreError = false
    )


}