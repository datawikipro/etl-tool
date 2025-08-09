package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate

import com.fasterxml.jackson.annotation.JsonInclude
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.{CoreTaskTemplateTargetDatabase, CoreTaskTemplateTargetFileSystem, CoreTaskTemplateTargetMessageBroker}

case class CoreTaskTemplateTarget(
                                   database: CoreTaskTemplateTargetDatabase = null,
                                   fileSystem: CoreTaskTemplateTargetFileSystem = null,
                                   messageBroker: CoreTaskTemplateTargetMessageBroker = null,
                                   ignoreError: Boolean
                                 ) {

}                    
                                      
