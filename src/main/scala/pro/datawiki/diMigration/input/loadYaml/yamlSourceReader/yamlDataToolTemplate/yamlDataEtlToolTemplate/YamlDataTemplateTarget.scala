package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.CoreTaskTemplateTarget
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.CoreTaskTemplateTargetDatabase
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget.{YamlDataTemplateTargetDatabase, YamlDataTemplateTargetFileSystem, YamlDataTemplateTargetMessageBroker}

case class YamlDataTemplateTarget(
                              database: YamlDataTemplateTargetDatabase=null,
                              fileSystem: YamlDataTemplateTargetFileSystem=null,
                              messageBroker: YamlDataTemplateTargetMessageBroker=null,
                              ignoreError: Boolean
                             ) {
  def getCoreTarget: CoreTaskTemplateTarget = CoreTaskTemplateTarget(
    database = database match {
      case null => null
      case fs =>  fs.getCoreTaskTemplateTargetDatabase
    },
    fileSystem = fileSystem match {
      case null => null
      case fs => fs.getCoreTaskTemplateTargetFileSystem
    },
    messageBroker = messageBroker match {
      case null => null
      case _ => throw Exception()
    },
    ignoreError = ignoreError
  )
}                    
                                      
