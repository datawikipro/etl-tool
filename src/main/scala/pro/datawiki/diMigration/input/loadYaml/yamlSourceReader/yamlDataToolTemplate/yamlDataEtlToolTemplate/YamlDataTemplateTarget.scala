package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.coreTaskTemplateTargetDatabase.CoreTaskTemplateTargetColumn
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.{CoreTaskTemplateDummy, CoreTaskTemplateTargetDatabase, CoreTaskTemplateTargetFileSystem}
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.{CoreTaskTemplateConnect, CoreTaskTemplateTarget}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetDatabase, YamlDataTemplateTargetDummy, YamlDataTemplateTargetFileSystem, YamlDataTemplateTargetMessageBroker}
import pro.datawiki.yamlConfiguration.LogicClass

case class YamlDataTemplateTarget(
                                   database: YamlDataTemplateTargetDatabase,
                                   fileSystem: YamlDataTemplateTargetFileSystem,
                                   messageBroker: YamlDataTemplateTargetMessageBroker,
                                   dummy: YamlDataTemplateTargetDummy,
                                   ignoreError: Boolean
                                 ) {
  def getCoreConnection: CoreTaskTemplateConnect = {
    LogicClass.getLogic(database, fileSystem, messageBroker, dummy) match {
      case x: YamlDataTemplateTargetDatabase => return x.connection.getCoreConnection
      case x: YamlDataTemplateTargetFileSystem => return x.connection.getCoreConnection
      case x: YamlDataTemplateTargetMessageBroker => return x.connection.getCoreConnection
      case fs => throw Exception()
    }

  }

  def getCoreTarget: CoreTaskTemplateTarget = CoreTaskTemplateTarget(
    database = database match {
      case null => null
      case fs => CoreTaskTemplateTargetDatabase(
        connection = fs.connection.getSourceName,
        source = fs.source,
        mode = fs.mode,
        partitionMode = fs.partitionMode,
        targetSchema = fs.targetSchema,
        targetTable = fs.targetTable,
        columns = fs.columns.map(x => CoreTaskTemplateTargetColumn(
          columnName = x.columnName,
          isNullable = x.isNullable,
          columnType = x.columnType,
          columnTypeDecode = x.columnTypeDecode

        )),
        uniqueKey = fs.uniqueKey,
        partitionBy = fs.partitionBy,
        scd = fs.scd
      )
    },
    fileSystem = fileSystem match {
      case null => null
      case fs => CoreTaskTemplateTargetFileSystem(
        connection = fs.connection.getSourceName,
        source = fs.source,
        tableName = fs.tableName,
        mode = fs.mode,
        targetFile = fs.targetFile,
        partitionBy = fs.partitionBy
      )
    },
    messageBroker = messageBroker match {
      case null => null
      case _ => throw IllegalArgumentException("Unexpected messageBroker value")
    },
    dummy = dummy match {
      case null => null
      case _ => CoreTaskTemplateDummy()
    },
    ignoreError = ignoreError
  )
}                    
                                      
