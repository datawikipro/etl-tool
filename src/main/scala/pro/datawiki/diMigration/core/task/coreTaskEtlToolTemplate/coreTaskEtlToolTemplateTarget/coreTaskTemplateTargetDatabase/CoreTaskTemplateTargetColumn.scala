package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.coreTaskTemplateTargetDatabase

import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

case class CoreTaskTemplateTargetColumn(columnName: String,
                                        isNullable: Boolean,
                                        columnType: String,
                                        columnTypeDecode: Boolean

                                       )