package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate

import com.fasterxml.jackson.annotation.JsonIgnore
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.CoreTaskTemplateSource
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource.coreTaskEtlToolTemplateSourceDBTable.CoreTaskTemplateSourceDBTableColumn
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource.coreTaskEtlToolTemplateSourceKafka.{CoreTaskTemplateSourceKafkaListTopics, CoreTaskTemplateSourceKafkaTopic, CoreTaskTemplateSourceKafkaTopicsByRegexp}
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource.{CoreTaskTemplateSourceBigQuery, CoreTaskTemplateSourceDBTable, CoreTaskTemplateSourceFileSystem, CoreTaskTemplateSourceKafka}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.*
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn

case class YamlDataTemplateSource(
                                   sourceName: String,
                                   objectName: String,
                                   @JsonIgnore
                                   segmentation: String = null,
                                   @JsonIgnore
                                   sourceDb: YamlDataTemplateSourceDBTable = null,
                                   @JsonIgnore
                                   sourceSQL: YamlDataTemplateSourceDBSQL = null,
                                   @JsonIgnore
                                   sourceFileSystem: YamlDataTemplateSourceFileSystem = null,
                                   @JsonIgnore
                                   sourceKafka: YamlDataTemplateSourceKafka = null,
                                   @JsonIgnore
                                   sourceWeb: YamlDataTemplateSourceWeb = null,
                                   @JsonIgnore
                                   sourceMail: YamlDataTemplateSourceMail = null,
                                   @JsonIgnore
                                   bigQuery: YamlDataTemplateSourceBigQuery = null,
                                   @JsonIgnore
                                   initMode: String,
                                   skipIfEmpty: Boolean = false
                                 ) {
  def getCoreSource: CoreTaskTemplateSource = CoreTaskTemplateSource(
    sourceName = sourceName,
    objectName = objectName,
    segmentation = segmentation match {
      case null => null
      case fs => throw NotImplementedException(s"Segmentation source functionality not implemented for source: $sourceName")
    },
    sourceDb = sourceDb match {
      case null => null
      case fs =>
        CoreTaskTemplateSourceDBTable(
          tableSchema = fs.tableSchema,
          tableName = fs.tableName,
          tableColumns = fs.tableColumns.map(col =>
            YamlConfigSourceDBTableColumn(
              columnName = col.columnName,
              columnType = col.columnType
            )
          ),
          filter = fs.filter,
          limit = fs.limit
        )
    },
    sourceSQL = sourceSQL match {
      case null => null
      case fs => throw NotImplementedException(s"SQL source functionality not implemented for source: $sourceName")
    },
    sourceFileSystem = sourceFileSystem match {
      case null => null
      case fs => CoreTaskTemplateSourceFileSystem(
        tableName = fs.tableName,
        tableColumns = fs.tableColumns.map(col =>
          CoreTaskTemplateSourceDBTableColumn(
            columnName = col.columnName,
            columnType = col.columnType
          )
        ),
        partitionBy = fs.partitionBy,
        where = fs.where,
        limit = fs.limit
      )
    },
    sourceKafka = sourceKafka match {
      case null => null
      case fs => CoreTaskTemplateSourceKafka(
        topics =
          fs.topics match {
            case null => null
            case fs2 =>
              CoreTaskTemplateSourceKafkaTopic(
                topicList = fs2.topicList
              )
          },
        listTopics =
          fs.listTopics match {
            case null => null
            case fs2 => CoreTaskTemplateSourceKafkaListTopics(
              template = fs2.template
            )
          },
        topicsByRegexp =
          fs.topicsByRegexp match {
            case null => null
            case fs2 => CoreTaskTemplateSourceKafkaTopicsByRegexp(
              template = fs2.template
            )
          }
      )
    },
    sourceWeb = sourceWeb match {
      case null => null
      case fs => throw NotImplementedException(s"Web source functionality not implemented for source: $sourceName")
    },
    sourceMail = sourceMail match {
      case null => null
      case fs => throw NotImplementedException(s"Mail source functionality not implemented for source: $sourceName")
    },
    bigQuery = bigQuery match {
      case null => null
      case fs => CoreTaskTemplateSourceBigQuery(
        projectId = fs.projectId,
        datasetId = fs.datasetId,
        tableId = fs.tableId,
      )
    },
    initMode = initMode,
    skipIfEmpty = skipIfEmpty
  )
}                    
                                      
