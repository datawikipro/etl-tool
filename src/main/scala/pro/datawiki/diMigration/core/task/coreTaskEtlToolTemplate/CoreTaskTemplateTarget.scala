package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate

import com.fasterxml.jackson.annotation.JsonInclude
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.{CoreTaskTemplateDummy, CoreTaskTemplateTargetDatabase, CoreTaskTemplateTargetFileSystem, CoreTaskTemplateTargetMessageBroker}

case class CoreTaskTemplateTarget(
                                   database: CoreTaskTemplateTargetDatabase,
                                   fileSystem: CoreTaskTemplateTargetFileSystem,
                                   messageBroker: CoreTaskTemplateTargetMessageBroker,
                                   dummy: CoreTaskTemplateDummy,
                                   ignoreError: Boolean
                                 ) {

}                    
                                      
