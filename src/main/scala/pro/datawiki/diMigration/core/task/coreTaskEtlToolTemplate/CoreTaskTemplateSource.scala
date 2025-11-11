package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource.*
import pro.datawiki.sparkLoader.configuration.YamlConfigSource
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.*
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceKafka.{YamlConfigSourceKafkaListTopics, YamlConfigSourceKafkaTopic, YamlConfigSourceKafkaTopicsByRegexp}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceWeb.YamlConfigSchema

case class CoreTaskTemplateSource(
                                   sourceName: String,
                                   objectName: String,
                                   segmentation: String = null,
                                   sourceDb: CoreTaskTemplateSourceDBTable = null,
                                   sourceSQL: CoreTaskTemplateSourceDBSQL = null,
                                   sourceFileSystem: CoreTaskTemplateSourceFileSystem = null,
                                   sourceKafka: CoreTaskTemplateSourceKafka = null,
                                   sourceWeb: CoreTaskTemplateSourceWeb = null,
                                   sourceMail: CoreTaskTemplateSourceMail = null,
                                   bigQuery: CoreTaskTemplateSourceBigQuery = null,
                                   initMode: String,
                                   skipIfEmpty: Boolean = false
                                 ) {
  def getEtToolFormat: YamlConfigSource = YamlConfigSource(
    sourceName = sourceName,
    objectName = objectName,
    segmentation = segmentation,
    sourceDb = sourceDb match {
      case null => null
      case fs =>
        YamlConfigSourceDBTable(
          tableSchema = fs.tableSchema,
          tableName = fs.tableName,
          tableColumns = fs.tableColumns,
          filter = fs.filter,
          limit = fs.limit
        )
    },
    sourceSQL = sourceSQL match {
      case null => null
      case fs =>
        YamlConfigSourceDBSQL(
          sql = sourceSQL.sql
        )
    },
    sourceFileSystem = sourceFileSystem match {
      case null => null
      case fs =>
        YamlConfigSourceFileSystem(
          tableName = fs.tableName,
          tableColumns = fs.tableColumns.map(col =>
            YamlConfigSourceDBTableColumn(
              columnName = col.columnName,
              columnType = col.columnType
            )
          ),
          partitionBy = fs.partitionBy,
          where = fs.where,
          limit = fs.limit)
    },
    sourceKafka = sourceKafka match {
      case null => null
      case fs => YamlConfigSourceKafka(
        topics = YamlConfigSourceKafkaTopic(topicList = fs.topics.topicList),
        listTopics = fs.listTopics match {
          case null => null
          case fs => YamlConfigSourceKafkaListTopics(template = fs.template)
        },
        topicsByRegexp = fs.topicsByRegexp match {
          case null => null
          case fs => YamlConfigSourceKafkaTopicsByRegexp(template = fs.template)
        }
      )
    },
    sourceWeb = sourceWeb match {
      case null => null
      case fs =>
        YamlConfigSourceWeb(
          run = fs.run,
          isDirty = fs.isDirty,
          schemas = fs.schemas.map(col => YamlConfigSchema(
            schemaName = col.schemaName,
            fileLocation = col.fileLocation,
            isError = col.isError,

          )
          ),
          validateStatusColumn = fs.validateStatusColumn,
          validateStatusValue = fs.validateStatusValue
        )
    },
    sourceMail = sourceMail match {
      case null => null
      case fs =>
        YamlConfigSourceMail(
          email = fs.email,
          password = fs.password,
          from = fs.from,
          subject = fs.subject)
    },
    sourceBigQuery = bigQuery match {
      case null => null
      case fs =>
        YamlConfigSourceBigQuery(
          projectId = fs.projectId,
          datasetId = fs.datasetId,
          tableId = fs.tableId
        )
    },
    initMode = initMode,
    skipIfEmpty = skipIfEmpty
  )
}                    
                                      
