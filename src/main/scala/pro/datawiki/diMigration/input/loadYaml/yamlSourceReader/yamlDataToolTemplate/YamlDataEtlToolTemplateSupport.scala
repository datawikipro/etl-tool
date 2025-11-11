package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.*
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.YamlDataTemplateSourceDBTable
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetColumn, YamlDataTemplateTargetDatabase, YamlDataTemplateTargetFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.YamlDataTemplateTransformationExtractAndValidateDataFrame
import pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation.YamlConfigEltOnServerSQL
import pro.datawiki.sparkLoader.connection.clickhouse.LoaderClickHouse
import pro.datawiki.sparkLoader.connection.databaseTrait.TableMetadata
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode.append
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, InitModeEnum, WriteMode}

class YamlDataEtlToolTemplateSupport(
                                      taskName: String,
                                      sourceCode: String,
                                      sourceTableSchema: String,
                                      sourceTableName: String,
                                      sourceLogicTableSchema: String,
                                      sourceLogicTableName: String,
                                      targetTableSchema: String,
                                      targetTableName: String,
                                      connection: String,
                                      yamlFileCoreLocation: String,
                                      yamlFileLocation: String
                                    ) {

  val dwhConfigLocation = "/opt/etl-tool/configConnection/postgres.yaml"
  val dwhLoader = LoaderPostgres(dwhConfigLocation)
  val dwhInfo = dwhLoader.getDwhConnectionInfo

  def getOdsTaskName: String = s"ods__${taskName}"

  def getOdsBatchTaskName: String = s"ods__batch__${taskName}"

  def getClickhouseTaskName: String = s"clickhouse__${taskName}"

  def getClickhouseBatchTaskName: String = s"clickhouse__batch__${taskName}"

  def getSnowflakeTaskName: String = s"snowflake__${taskName}"

  def getSnowflakeBatchTaskName: String = s"snowflake__batch__${taskName}"

  def getOdsYamlFile: String = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__${sourceCode}/ods/$taskName.yaml"

  def getClickhouseYamlFile: String = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__${sourceCode}/clickhouse/$taskName.yaml"

  def getSnowflakeYamlFile: String = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__${sourceCode}/snowflake/$taskName.yaml"

  def getJsonStreamDataWarehouse: YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = "datewarehouse",
    connection = "minioJsonStream",
    configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
  )

  def getJsonDataWarehouse: YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = "datewarehouseJson",
    connection = ConnectionEnum.minioJson.toString,
    configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
  )

  def getParquetDataWarehouse: YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = "datewarehouseParquet",
    connection = ConnectionEnum.minioParquet.toString,
    configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
  )

  def getPostgres: YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = "postgres",
    connection = "postgres",
    configLocation = "/opt/etl-tool/configConnection/postgres.yaml"
  )

  def getAmazonS3: YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = "datewarehouse",
    connection = ConnectionEnum.minioParquet.toString,
    configLocation = "/opt/etl-tool/configConnection/s3.yaml" //TODO
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

  def getOdsYamlDataTemplateTarget(metadata: TableMetadata): YamlDataTemplateTarget =
    YamlDataTemplateTarget(
      database = YamlDataTemplateTargetDatabase(
        connection = getMainDataWarehouseName,
        source = "src",
        mode = metadata.columns.isEmpty match {
          case true => WriteMode.overwritePartition
          case false => WriteMode.mergeDelta
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
          s"""CREATE TABLE IF NOT EXISTS ods__${targetTableSchema}.${targetTableName}
             |(
             |    ${metadata.columns.map(col => s"${col.column_name} ${LoaderClickHouse.encodeIsNullable(col.isNullable, LoaderClickHouse.encodeDataType(col.data_type))}").mkString(",\n    ")}, valid_from_dttm Datetime, valid_to_dttm Datetime
             |)
             |    ENGINE = MergeTree ORDER BY (${metadata.primaryKey.mkString(",")})  SETTINGS index_granularity = 8192;""".stripMargin,
        ),
      ),
      ignoreError = false
    )

  def getClickhouseYamlConfigEltOnServerOperationPost(metadata: TableMetadata): YamlConfigEltOnServerOperation =
    YamlConfigEltOnServerOperation(
      eltOnServerOperationName = "preSql",
      sourceName = "clickhouseUnico",
      sql = YamlConfigEltOnServerSQL(
        sql = List.apply(s"""OPTIMIZE TABLE ods__${targetTableSchema}.${targetTableName} FINAL;""".stripMargin),
      ),
      ignoreError = false
    )

  def getClickhouseTarget(inConnectionName: String, inSourceName: String, metadata: TableMetadata): YamlDataTemplateTarget = YamlDataTemplateTarget(
    database = YamlDataTemplateTargetDatabase(
      connection = inConnectionName,
      source = inSourceName,
      mode = append,
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
      partitionBy = null,
      scd = "SCD_0"
    ),
    messageBroker = null,
    dummy = null,
    fileSystem = null,
    ignoreError = false
  )

  def getSnowflakeTarget(inConnectionName: String, inSourceName: String): YamlDataTemplateTarget = YamlDataTemplateTarget(
    database = null,
    messageBroker = null,
    dummy = null,
    fileSystem = YamlDataTemplateTargetFileSystem(
      connection = inConnectionName,
      source = inSourceName,
      mode = WriteMode.overwritePartition,
      targetFile = s"dwh-backups/ods__${targetTableSchema}/${targetTableName}",
      partitionBy = List.apply("run_id"),
    ),
    ignoreError = false
  )


  def getClickhouseConfig: YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = "clickhouseUnico",
    connection = "clickhouse",
    configLocation = "/opt/etl-tool/configConnection/clickhouse.yaml"
  )

  def getDataForDM: YamlDataTemplateSource = YamlDataTemplateSource(
    getPostgres.getSourceName,
    objectName = "src",
    sourceDb = YamlDataTemplateSourceDBTable(
      tableSchema = s"ods__$targetTableSchema",
      tableName = targetTableName,
      tableColumns = List.apply(),
      filter = s"run_id = '$${run_id}'"
    ),
    initMode = InitModeEnum.instantly.toString
  )

}