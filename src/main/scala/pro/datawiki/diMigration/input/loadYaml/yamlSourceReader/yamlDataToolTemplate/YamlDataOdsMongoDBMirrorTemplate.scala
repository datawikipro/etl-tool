package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.dag.{CoreBaseDag, CoreDag}
import pro.datawiki.diMigration.core.dictionary.Schedule
import pro.datawiki.diMigration.core.task.CoreTask
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceDBTable, YamlDataTemplateSourceFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetColumn, YamlDataTemplateTargetDatabase, YamlDataTemplateTargetFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.{YamlDataTemplateTransformationExtractAndValidateDataFrame, YamlDataTemplateTransformationSparkSql}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.{YamlDataTemplateConnect, YamlDataTemplateSource, YamlDataTemplateTarget, YamlDataTemplateTransformation}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{Metadata, YamlDataTaskToolTemplate, YamlDataToolTemplate}
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataColumn, TableMetadataType}
import pro.datawiki.sparkLoader.connection.mongodb.LoaderMongoDb
import pro.datawiki.sparkLoader.connection.{FileStorageTrait, NoSQLDatabaseTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.{InitModeEnum, PartitionModeEnum, WriteMode}
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

case class YamlDataOdsMongoDBMirrorTemplate(
                                             taskName: String=throw Exception(),
                                             yamlFileCoreLocation: String=throw Exception(),
                                             yamlFileLocation: String=throw Exception(),
                                             connection: String=throw Exception(),
                                             configLocation: String=throw Exception(),
                                             metadataConnection: String=throw Exception(),
                                             metadataConfigLocation: String=throw Exception(),
                                             sourceSchema: String=throw Exception(),
                                             sourceTable: String=throw Exception(),
                                             tableSchema: String=throw Exception(),
                                             tableName: String=throw Exception(),
                                           ) extends YamlDataTaskToolTemplate {
  override def getCoreTask: List[CoreTask] = {
    val connectionTrait = NoSQLDatabaseTrait.apply(connection, configLocation)
    val metadata = Metadata(metadataConnection, metadataConfigLocation, s"ods__$tableSchema", tableName)

    val stg = new YamlDataEtlToolTemplate(
      taskName = s"stg__batch__${taskName}",
      yamlFile = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__monga/stg/${sourceTable}.yaml",
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "mongodb",
          connection = "mongodb",
          configLocation = configLocation
        ),
        YamlDataTemplateConnect(
          sourceName = "datewarehouse",
          connection = "minioJson",
          configLocation = "/opt/etl-tool/configConnection/minio.yaml" //TODO
        ),
      ),
      preEtlOperations = List.empty,
      sources = List.apply(
        YamlDataTemplateSource(
          sourceName = "mongodb",
          objectName = "src",
          sourceDb = YamlDataTemplateSourceDBTable(
            tableSchema = sourceSchema,
            tableName = sourceTable,
            tableColumns = List.apply(), //metadata.columns.map(col => YamlConfigSourceDBTableColumn(columnName = col.column_name)),
          ),
          initMode = InitModeEnum.instantly.toString
        )
      ),
      transform = List.apply(
        YamlDataTemplateTransformation(
          objectName = "schema",
          cache = null,
          idMap = null,
          sparkSql = null,
          sparkSqlLazy = null,
          extractSchema = null,
          extractAndValidateDataFrame = YamlDataTemplateTransformationExtractAndValidateDataFrame(
            dataFrameIn = "src",
            configLocation = s"/opt/etl-tool/configMigrationSchemas/mongodb__${sourceSchema}__$sourceTable.yaml"
          ),
          adHoc = null
        ),
      ),
      target = List.apply(
        YamlDataTemplateTarget(
          database = null,
          messageBroker = null,
          dummy = null,
          fileSystem = YamlDataTemplateTargetFileSystem(
            connection = "datewarehouse",
            source = "src",
            mode = WriteMode.overwritePartition,
            partitionMode = PartitionModeEnum.direct.toString,
            targetFile = s"stg/${tableSchema}/${sourceTable}",
            partitionBy = List.apply("run_id"),
          ),
          ignoreError = false
        )
      ),
      dependencies = List.empty
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = s"ods__batch__${taskName.replace("-", "_")}",
      yamlFile = s"${yamlFileCoreLocation}/ods__${yamlFileLocation}__monga/ods/$sourceTable.yaml",
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "datewarehouse",
          connection = "minioJson",
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
          objectName = "src",
          sourceFileSystem = YamlDataTemplateSourceFileSystem(
            tableName = s"stg/${tableSchema}/${sourceTable}",
            tableColumns = List.apply(),
            partitionBy = List.apply("run_id"),
            where = null,
            limit = 0
          ),

          initMode = InitModeEnum.instantly.toString
        )
      ),
      transform = List.apply(
        YamlDataTemplateTransformation(
          objectName = "level1",
          cache = null,
          idMap = null,
          sparkSql = YamlDataTemplateTransformationSparkSql(
            sql = s"select ${
              metadata.columns.map(col => {
                if col.column_name == "id" then s"_id as id" else s"${col.column_name} as ${col.column_name}"
              }).mkString(",")
            } from src",
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
            source = "level1",
            mode = metadata.columns.isEmpty match {
              case true => WriteMode.overwritePartition
              case false => WriteMode.merge
            },
            partitionMode = null, //TODO
                        targetSchema = s"ods__${tableSchema}",
            targetTable = s"${tableName}",
            columns = metadata.columns.map(col =>
              YamlDataTemplateTargetColumn(
                columnName = col.column_name,
                isNullable = true,
                columnType = YamlDataOdsMongoDBMirrorTemplate.encodeDataType(col.data_type),
                columnTypeDecode = false
              )),
            uniqueKey = metadata.primaryKey,
            deduplicationKey = List.apply(),
            partitionBy = null
          ),
          ignoreError = false
        )
      ),
      dependencies = List.apply(s"stg__batch__${taskName}")
    )

    return stg.getCoreTask ++ ods.getCoreTask
  }
}


object YamlDataOdsMongoDBMirrorTemplate extends YamlClass {
  def apply(inConfig: String, row: mutable.Map[String, String]): YamlDataOdsMongoDBMirrorTemplate = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsMongoDBMirrorTemplate = mapper.readValue(text, classOf[YamlDataOdsMongoDBMirrorTemplate])
    return configYaml
  }

  def encodeDataType(in: TableMetadataType): String = {
    return in match {
      case TableMetadataType.Integer => return "Int32"
      case TableMetadataType.Bigint => return "Int64"
      case TableMetadataType.String => return "String"
      case TableMetadataType.Boolean => return "UInt8"
      case TableMetadataType.Varchar => return "String"
      case TableMetadataType.Date => return "Date"
      case TableMetadataType.DoublePrecision => return "Float64"
      case TableMetadataType.Numeric => return "Float64"
      case TableMetadataType.Real => return "Float32"
      case TableMetadataType.Text => return "String"
      case TableMetadataType.TimestampWithTimeZone => return "DateTime"
      case TableMetadataType.TimestampWithoutTimeZone => return "DateTime"
      case _ => throw UnsupportedOperationException("Unsupported table metadata type")
    }
  }
}