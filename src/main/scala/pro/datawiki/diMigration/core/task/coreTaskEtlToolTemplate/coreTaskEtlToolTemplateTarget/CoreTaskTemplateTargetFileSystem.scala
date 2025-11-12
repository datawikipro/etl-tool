package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget

import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

case class CoreTaskTemplateTargetFileSystem(
                                             connection: String,
                                             source: String,
                                             tableName: String,
                                             mode: WriteMode,
                                             targetFile: String,
                                             partitionBy: List[String] = List.apply(),
                                           )