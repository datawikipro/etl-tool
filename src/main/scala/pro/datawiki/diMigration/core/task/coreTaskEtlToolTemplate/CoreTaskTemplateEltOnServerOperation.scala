package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskTemplateEltOnServerSQL.CoreTaskTemplateEltOnServerSQL

case class CoreTaskTemplateEltOnServerOperation(
                                                 eltOnServerOperationName: String,
                                                 sourceName: String,
                                                 sql: CoreTaskTemplateEltOnServerSQL,
                                                 ignoreError: Boolean
                                               )
