package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget

import pro.datawiki.sparkLoader.connection.WriteMode

case class CoreTaskTemplateTargetColumn(columnName: String,
                                  isNewCCD: Boolean,
                                  domainName: String,
                                  tenantName: String,
                                  isNullable: Boolean
                                 )