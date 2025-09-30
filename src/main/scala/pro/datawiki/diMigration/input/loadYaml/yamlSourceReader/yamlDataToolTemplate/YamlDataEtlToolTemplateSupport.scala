package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceBigQuery, YamlDataTemplateSourceDBTable, YamlDataTemplateSourceFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetColumn, YamlDataTemplateTargetDatabase, YamlDataTemplateTargetFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.YamlDataTemplateTransformationExtractAndValidateDataFrame
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.{YamlDataTemplateConnect, YamlDataTemplateSource, YamlDataTemplateTarget, YamlDataTemplateTransformation}
import pro.datawiki.sparkLoader.connection.databaseTrait.TableMetadata
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
  def getStgTaskName: String = s"stg__batch__${taskName}"

  def getOdsTaskName: String = s"ods__batch__${taskName}"

  def getStgYamlFile: String = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__bigQuery/stg/$taskName.yaml"

  def getOdsYamlFile: String = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__bigQuery/ods/$taskName.yaml"


  def getParquetDataWarehouse: YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = "datewarehouse",
    connection = ConnectionEnum.minioParquet.toString,
    configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
  )

  def getMainDataWarehouseName: String = "datewarehouseMain"

  def getMainDataWarehouse: YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = getMainDataWarehouseName,
    connection = ConnectionEnum.postgres.toString,
    configLocation = "/opt/etl-tool/configConnection/postgres.yaml" //TODO
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
      adHoc = null
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
        targetFile = s"stg/${sourceLogicTableSchema}/${sourceLogicTableName}",
        partitionBy = List.apply("run_id"),
      ),
      ignoreError = false
    )

  def getOdsYamlDataTemplateSourceYamlDataTemplateSource: YamlDataTemplateSource = YamlDataTemplateSource(
    sourceName = "datewarehouse",
    objectName = "src",
    sourceFileSystem = YamlDataTemplateSourceFileSystem(
      tableName = s"stg/${sourceTableSchema}/${sourceTableName}",
      tableColumns = List.apply(),
      partitionBy = List.apply("run_id"),
      where = null,
      limit = 0
    ),
    initMode = InitModeEnum.instantly.toString
  )

  def getOdsYamlDataTemplateTarget(metadata: TableMetadata): YamlDataTemplateTarget = YamlDataTemplateTarget(
    database = YamlDataTemplateTargetDatabase(
      connection = "postgres",
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
      partitionBy = null
    ),
    fileSystem = null,
    messageBroker = null,
    dummy = null,
    ignoreError = false
  )
}