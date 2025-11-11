package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource.coreTaskTemplateSourceWeb.CoreTaskTemplateSourceWebConfigSchema

case class CoreTaskTemplateSourceWeb(
                                      run: String,
                                      isDirty: Boolean,
                                      schemas: List[CoreTaskTemplateSourceWebConfigSchema],
                                      validateStatusColumn:String,
                                      validateStatusValue: List[String]
                                    )