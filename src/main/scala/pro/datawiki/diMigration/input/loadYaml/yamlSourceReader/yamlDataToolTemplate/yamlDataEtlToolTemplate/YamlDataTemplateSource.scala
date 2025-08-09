package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate

import com.fasterxml.jackson.annotation.JsonIgnore
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.CoreTaskTemplateSource
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource.coreTaskEtlToolTemplateSourceDBTable.CoreTaskTemplateSourceDBTableColumn
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource.{CoreTaskTemplateSourceDBTable, CoreTaskTemplateSourceFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.*
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.{YamlConfigSourceDBTableColumn, YamlConfigSourceDBTablePartition}

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
                                   cache: String = null,
                                   @JsonIgnore
                                   initMode: String,
                                   skipIfEmpty: Boolean = false
                                 ) {
  def getCoreSource: CoreTaskTemplateSource = CoreTaskTemplateSource(
    sourceName = sourceName,
    objectName = objectName,
    segmentation = segmentation match {
      case null => null
      case fs => throw Exception()
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
          partitionBy = fs.partitionBy.map(col =>
            YamlConfigSourceDBTablePartition(
              partitionMode = col.partitionMode,
              columnName = col.columnName
            )
          ),
          filter = fs.filter,
          limit = fs.limit
        )
    },
    sourceSQL = sourceSQL match {
      case null => null
      case fs => throw Exception()
    },
    sourceFileSystem = sourceFileSystem match {
      case null => null
      case fs => CoreTaskTemplateSourceFileSystem(
        tableName = fs.tableName,
        tableColumns = fs.tableColumns.map(col=>
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
      case fs => throw Exception()
    },
    sourceWeb = sourceWeb match {
      case null => null
      case fs => throw Exception()
    },
    sourceMail = sourceMail match {
      case null => null
      case fs => throw Exception()
    },
    cache = cache,
    initMode = initMode,
    skipIfEmpty = skipIfEmpty
  )
}                    
                                      
