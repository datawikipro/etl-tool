package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.CoreTaskTemplateTargetFileSystem
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.YamlDataTemplateConnect
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

case class YamlDataTemplateTargetFileSystem(
                                             connection: YamlDataTemplateConnect,
                                             source: String,
                                             tableName: String,
                                             mode: WriteMode,
                                             targetFile: String,
                                             partitionBy: List[String] = List.apply(),
                                           )