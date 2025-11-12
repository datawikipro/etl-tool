package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplateSupport

import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceDBTable, YamlDataTemplateSourceFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.YamlDataTemplateTargetFileSystem
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.{YamlDataTemplateTransformationExtractAndValidateDataFrame, YamlDataTemplateTransformationExtractSchema}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.{YamlDataTemplateConnect, YamlDataTemplateSource, YamlDataTemplateTarget, YamlDataTemplateTransformation}
import pro.datawiki.sparkLoader.dictionaryEnum.{InitModeEnum, WriteMode}

class YamlDataEtlToolTemplateSupportStg(taskName: String,
                                        sourceLogicTableSchema: String,
                                        sourceLogicTableName: String,
                                        yamlFileCoreLocation: String,
                                        yamlFileLocation: String,
                                        sourceCode: String
                                       ) {

  def getStgFolder: String = s"stg/${sourceLogicTableSchema}/${sourceLogicTableName}/$sourceCode/run_id=$${locationBasedOnRunId}"

  def getStgYamlFile: String = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__${sourceCode}/stg/$taskName.yaml"

  def getStgTaskName: String = s"stg__${taskName}"

  def getStgBatchTaskName: String = s"stg__batch__${taskName}"

  def getSrcFromRemote(sourceName: YamlDataTemplateConnect,sourceSchema: String, sourceTable: String): YamlDataTemplateSource = {
    YamlDataTemplateSource(
      sourceName = sourceName,
      objectName = "src",
      sourceDb = YamlDataTemplateSourceDBTable(
        tableSchema = sourceSchema,
        tableName = sourceTable,
        tableColumns = List.empty, //metadata.columns.map(col => YamlConfigSourceDBTableColumn(columnName = col.column_name)),
      ),
      initMode = InitModeEnum.instantly.toString
    )
  }


  def getStgYamlDataTemplateTarget(inConnection: YamlDataTemplateConnect, inSource: String, partitionBy: List[String]): YamlDataTemplateTarget =
    YamlDataTemplateTarget(
      database = null,
      messageBroker = null,
      dummy = null,
      fileSystem = YamlDataTemplateTargetFileSystem(
        connection = inConnection,
        source = inSource,
        tableName = s"stg__${sourceLogicTableSchema}.${sourceLogicTableName}",
        mode = WriteMode.overwritePartition,
        targetFile = getStgFolder,
        partitionBy = partitionBy,
      ),
      ignoreError = false
    )

  def getKafkaYamlDataTemplateSource(inSourceName: YamlDataTemplateConnect, inTableLocation: String): YamlDataTemplateSource =
    YamlDataTemplateSource(
      sourceName = inSourceName,
      objectName = "source",
      sourceFileSystem = YamlDataTemplateSourceFileSystem(
        tableName = inTableLocation,
        tableColumns = List.empty,
        partitionBy = List.apply("locationBasedOnRunId"),
        where = null,
        limit = 0
      ),
      skipIfEmpty = true,
      initMode = InitModeEnum.instantly.toString
    )

  def getOdsYamlDataTemplateSource(inSourceName: YamlDataTemplateConnect): YamlDataTemplateSource =
    YamlDataTemplateSource(
      sourceName = inSourceName,
      objectName = "src",
      sourceFileSystem = YamlDataTemplateSourceFileSystem(
        tableName = getStgFolder,
        tableColumns = List.empty,
        partitionBy = List.empty,
        where = null,
        limit = 0
      ),
      initMode = InitModeEnum.instantly.toString
    )

  def getExtractSchema(inObjectData: String, inSourceObjectData: String, inColumnNameBefore: String,inColumnNameAfter: String, sourceName: String): YamlDataTemplateTransformation =
    YamlDataTemplateTransformation(
      objectName = inObjectData,
      cache = null,
      idMap = null,
      sparkSql = null,
      sparkSqlLazy = null,
      extractSchema = YamlDataTemplateTransformationExtractSchema(
        tableName = inSourceObjectData,
        jsonColumn = inColumnNameBefore,
        jsonResultColumn = inColumnNameAfter,
        baseSchema = s"/opt/etl-tool/configMigrationSchemas/stg__${sourceName}_$inColumnNameBefore.yaml",
        mergeSchema = true,
      ),
      extractAndValidateDataFrame = null,
      adHoc = null,
      deduplicate = null
    )

  def getExtractAndValidate(inObjectName:String, inTableIn:String):YamlDataTemplateTransformation = YamlDataTemplateTransformation(
    objectName = inObjectName,
    cache = null,
    idMap = null,
    sparkSql = null,
    sparkSqlLazy = null,
    extractSchema = null,
    extractAndValidateDataFrame = YamlDataTemplateTransformationExtractAndValidateDataFrame(
      dataFrameIn = inTableIn,
      configLocation = s"/opt/etl-tool/configMigrationSchemas/${sourceCode}__${sourceLogicTableSchema}__$sourceLogicTableName.yaml"
    ),
    adHoc = null,
    deduplicate = null
  )

  def writeStg(mode: WriteMode, inSource: String, inConnection: YamlDataTemplateConnect): YamlDataTemplateTarget =
    YamlDataTemplateTarget(
      database = null,
      fileSystem = YamlDataTemplateTargetFileSystem(
        connection = inConnection,
        source = inSource,
        tableName = s"stg__$sourceLogicTableSchema.\"${sourceLogicTableName}\"",
        mode = WriteMode.autoOverwrite,
        targetFile = getStgFolder,
        partitionBy = List("business_date"),
      ),
      messageBroker = null,
      dummy = null,

      ignoreError = false
    )


  def getMonga(configLocation: String): YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = "mongodb",
    connection = "mongodb",
    configLocation = configLocation
  )

  def getPostgres(configLocation: String): YamlDataTemplateConnect = YamlDataTemplateConnect(
    sourceName = "src",
    connection = "postgres",
    configLocation = configLocation
  )

}
