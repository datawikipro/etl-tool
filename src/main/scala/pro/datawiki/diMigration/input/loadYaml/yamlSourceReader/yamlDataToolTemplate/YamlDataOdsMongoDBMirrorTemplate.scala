package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate

import pro.datawiki.diMigration.core.dag.{CoreBaseDag, CoreDag}
import pro.datawiki.diMigration.core.dictionary.Schedule
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.YamlDataToolTemplate
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.{YamlDataTemplateSourceDBTable, YamlDataTemplateSourceFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetColumn, YamlDataTemplateTargetDatabase, YamlDataTemplateTargetFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.{YamlDataTemplateTransformationExtractAndValidateDataFrame, YamlDataTemplateTransformationSparkSql}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.{YamlDataTemplateConnect, YamlDataTemplateSource, YamlDataTemplateTarget, YamlDataTemplateTransformation}
import pro.datawiki.sparkLoader.configuration.{InitModeEnum, PartitionModeEnum}
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataColumn}
import pro.datawiki.sparkLoader.connection.{FileStorageTrait, NoSQLDatabaseTrait, WriteMode}
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

case class YamlDataOdsMongoDBMirrorTemplate(
                                             dagName: String,
                                             taskName: String,
                                             description: String,
                                             yamlFileLocation: String,
                                             pythonFile: String,
                                             connection: String,
                                             configLocation: String,
                                             metadataConnection: String,
                                             metadataConfigLocation: String,
                                             sourceSchema: String,
                                             sourceTable: String,
                                             tableSchema: String,
                                             tableName: String,
                                           ) extends YamlDataToolTemplate {
  override def getCoreDag: List[CoreDag] = {
    val connectionTrait = NoSQLDatabaseTrait.apply(connection, configLocation)
    val metadataConnectionTrait = FileStorageTrait.apply(metadataConnection, metadataConfigLocation)
    val list = metadataConnectionTrait.readDf("metadata/tables").filter(s"table_name = '$tableName'").select("columns", "primary_key_columns").collect()

    val columns: List[String] = {
      if list.nonEmpty then list.map(row => row.getAs[String]("columns")).head.split(",").toList
      else List.apply()
    }
    val primary_key_columns: List[String] = {
      if list.nonEmpty then list.map(row => row.getAs[String]("primary_key_columns")).head.split(",").toList
      else List.apply()
    }
    val metadata: TableMetadata =
      TableMetadata(
        columns.map(col =>
          TableMetadataColumn(
            column_name = col,
            data_type = null
          )
        ),
        primary_key_columns
      )


    val stg = new YamlDataEtlToolTemplate(
      taskName = s"stg__batch__${taskName}",
      yamlFile = s"$yamlFileLocation/stg/$taskName.yaml",
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "mongodb",
          connection = "mongodb",
          configLocation = configLocation
        ),
        YamlDataTemplateConnect(
          sourceName = "datewarehouse",
          connection = "minioJson",
          configLocation = "/opt/etl-tool/config/connection/minio.yaml" //TODO
        ),
      ),
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
          extractSchema = null,
          extractAndValidateDataFrame = YamlDataTemplateTransformationExtractAndValidateDataFrame(
            dataFrameIn = "src",
            configLocation = s"/opt/etl-tool/config/schemas/mongodb/$sourceSchema/$sourceTable.yaml"
          ),
          adHoc = null
        ),
      ),
      target = List.apply(
        YamlDataTemplateTarget(
          fileSystem = YamlDataTemplateTargetFileSystem(
            connection = "datewarehouse",
            source = "src",
            mode = WriteMode.overwrite,
            partitionMode = PartitionModeEnum.direct.toString,
            targetFile = s"stg/${tableSchema}/${sourceTable}",
            partitionBy = List.apply("run_id"),
          ),
          ignoreError = false
        )
      )
    )

    val ods = new YamlDataEtlToolTemplate(
      taskName = s"ods__batch__{$taskName}",
      yamlFile = s"$yamlFileLocation/ods/$taskName.yaml",
      connections = List.apply(
        YamlDataTemplateConnect(
          sourceName = "datewarehouse",
          connection = "minioJson",
          configLocation = "/opt/etl-tool/config/connection/minio.yaml" //TODO
        ),
        YamlDataTemplateConnect(
          sourceName = "postgres",
          connection = "postgres",
          configLocation = "/opt/etl-tool/config/connection/postgres.yaml"
        ),
      ),
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
            sql = s"select ${metadata.columns.map(col => {
              if col.column_name == "id" then s"_id as id" else s"${col.column_name} as ${col.column_name}"
            }).mkString(",")} from src",
          ),
          extractSchema = null,
          extractAndValidateDataFrame = null,
          adHoc = null
        ),
      ),
      target = List.apply(
        YamlDataTemplateTarget(
          database = YamlDataTemplateTargetDatabase(
            connection = "postgres",
            source = "level1",
            mode = metadata.columns.isEmpty match {
              case true => WriteMode.overwrite
              case false => WriteMode.merge
            },
            partitionMode = null, //TODO
            targetTable = s"ods__${tableSchema}.${tableName}",
            columns = metadata.columns.map(col =>
              YamlDataTemplateTargetColumn(
                columnName = col.column_name,
                isNewCCD = false,
                domainName = null,
                tenantName = null,
                isNullable = true
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

    return List.apply(CoreBaseDag(
      dagName = dagName,
      tags = List.apply("ods", s"$tableSchema"),
      dagDescription = description,
      schedule = Schedule.None,
      pythonFile = pythonFile,
      listTask = stg.getCoreTask ++ ods.getCoreTask,
    ))
  }
}


object YamlDataOdsMongoDBMirrorTemplate extends YamlClass {
  def apply(inConfig: String, row: mutable.Map[String, String]): YamlDataOdsMongoDBMirrorTemplate = {
    val text: String = getLines(inConfig, row)
    val configYaml: YamlDataOdsMongoDBMirrorTemplate = mapper.readValue(text, classOf[YamlDataOdsMongoDBMirrorTemplate])
    return configYaml
  }
}