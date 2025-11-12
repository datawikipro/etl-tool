package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate

import com.fasterxml.jackson.annotation.JsonInclude
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.{CoreTaskTemplateDummy, CoreTaskTemplateTargetDatabase, CoreTaskTemplateTargetFileSystem, CoreTaskTemplateTargetMessageBroker}
import pro.datawiki.sparkLoader.configuration.YamlConfigTarget
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.yamlConfigTargetDatabase.YamlConfigTargetColumn
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.{YamlConfigTargetDatabase, YamlConfigTargetDummy, YamlConfigTargetFileSystem, YamlConfigTargetMessageBroker}

case class CoreTaskTemplateTarget(
                                   database: CoreTaskTemplateTargetDatabase,
                                   fileSystem: CoreTaskTemplateTargetFileSystem,
                                   messageBroker: CoreTaskTemplateTargetMessageBroker,
                                   dummy: CoreTaskTemplateDummy,
                                   ignoreError: Boolean
                                 ) {
  def getEtToolFormat: YamlConfigTarget = YamlConfigTarget(
    database = database match {
      case null => null
      case fs =>
        YamlConfigTargetDatabase(
          connection = fs.connection,
          source = fs.source,
          mode = fs.mode.getExportString,
          targetSchema = fs.targetSchema,
          targetTable = fs.targetTable,
          columns = fs.columns.map(col =>
            YamlConfigTargetColumn(
              columnName = col.columnName,
              isNullable = col.isNullable,
              columnType = col.columnType,
              columnTypeDecode = col.columnTypeDecode
            )
          ),
          uniqueKey = fs.uniqueKey,
          partitionBy = fs.partitionBy,
          scd = fs.scd)
    },
    fileSystem = fileSystem match {
      case null => null
      case fs =>
        YamlConfigTargetFileSystem(
          connection = fs.connection,
          source = fs.source,
          tableName= fs.tableName,
          mode = fs.mode.getExportString,
          targetFile = fs.targetFile,
          partitionBy = fs.partitionBy
        )
    },
    messageBroker = messageBroker match {
      case null => null
      case fs =>
        YamlConfigTargetMessageBroker(
          connection = fs.connection,
          source = fs.source,
          mode = fs.mode,
          target = fs.target,
          partitionMode = fs.partitionMode
        )
    },
    dummy = dummy match {
      case null => null
      case fs => YamlConfigTargetDummy()
    },
    ignoreError = ignoreError
  )
}                    
                                      
