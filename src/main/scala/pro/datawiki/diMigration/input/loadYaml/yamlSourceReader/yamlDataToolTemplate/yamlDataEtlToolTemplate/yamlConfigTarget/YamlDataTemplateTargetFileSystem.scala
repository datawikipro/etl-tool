package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.CoreTaskTemplateTargetFileSystem
import pro.datawiki.sparkLoader.connection.WriteMode

case class YamlDataTemplateTargetFileSystem(
                                             connection: String,
                                             source: String,
                                             mode: WriteMode,
                                             partitionMode: String,
                                             targetFile: String,
                                             partitionBy: List[String] = List.apply(),
                                     ) {
  def getCoreTaskTemplateTargetFileSystem:CoreTaskTemplateTargetFileSystem={
    return CoreTaskTemplateTargetFileSystem(connection,source,mode,partitionMode,targetFile,partitionBy)
  }
}