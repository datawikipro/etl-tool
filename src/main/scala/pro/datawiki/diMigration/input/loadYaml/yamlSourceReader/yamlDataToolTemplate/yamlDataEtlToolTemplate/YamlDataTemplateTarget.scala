package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.CoreTaskTemplateTarget
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.coreTaskTemplateTargetDatabase.CoreTaskTemplateTargetColumn
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.{CoreTaskTemplateDummy, CoreTaskTemplateTargetDatabase, CoreTaskTemplateTargetFileSystem}
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetDatabase, YamlDataTemplateTargetDummy, YamlDataTemplateTargetFileSystem, YamlDataTemplateTargetMessageBroker}

case class YamlDataTemplateTarget(
                                   database: YamlDataTemplateTargetDatabase,
                                   fileSystem: YamlDataTemplateTargetFileSystem,
                                   messageBroker: YamlDataTemplateTargetMessageBroker,
                                   dummy: YamlDataTemplateTargetDummy,
                                   ignoreError: Boolean
                                 ) {
  def getCoreTarget: CoreTaskTemplateTarget = CoreTaskTemplateTarget(
    database = database match {
      case null => null
      case fs => CoreTaskTemplateTargetDatabase(
        connection = fs.connection,
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
        deduplicationKey = fs.deduplicationKey,
        partitionBy = fs.partitionBy
      )
    },
    fileSystem = fileSystem match {
      case null => null
      case fs => CoreTaskTemplateTargetFileSystem(
        connection = fs.connection,
        source = fs.source,
        mode = fs.mode,
        partitionMode = fs.partitionMode,
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
                                      
