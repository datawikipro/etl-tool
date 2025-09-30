package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource.*

case class CoreTaskTemplateSource(
                                   sourceName: String,
                                   objectName: String,
                                   segmentation: String = null,
                                   sourceDb: CoreTaskTemplateSourceDBTable = null,
                                   sourceSQL: CoreTaskTemplateSourceDBSQL = null,
                                   sourceFileSystem: CoreTaskTemplateSourceFileSystem = null,
                                   sourceKafka: CoreTaskTemplateSourceKafka = null,
                                   sourceWeb: CoreTaskTemplateSourceWeb = null,
                                   sourceMail: CoreTaskTemplateSourceMail = null,
                                   bigQuery: CoreTaskTemplateSourceBigQuery = null,
                                   initMode: String,
                                   skipIfEmpty: Boolean = false
                                 ) {

}                    
                                      
