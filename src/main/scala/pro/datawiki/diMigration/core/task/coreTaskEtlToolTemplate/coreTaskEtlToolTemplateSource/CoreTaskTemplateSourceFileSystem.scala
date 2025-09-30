package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource.coreTaskEtlToolTemplateSourceDBTable.CoreTaskTemplateSourceDBTableColumn

case class CoreTaskTemplateSourceFileSystem(
                                             tableName: String,
                                             tableColumns: List[CoreTaskTemplateSourceDBTableColumn],
                                             partitionBy: List[String] = List.apply(),
                                             where: String,
                                             limit: Int
                                           )