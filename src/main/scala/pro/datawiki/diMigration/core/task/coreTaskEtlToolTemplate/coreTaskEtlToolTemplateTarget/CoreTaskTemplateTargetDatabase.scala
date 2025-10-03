package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget

import org.apache.spark.sql.functions.*
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.coreTaskTemplateTargetDatabase.CoreTaskTemplateTargetColumn
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

case class CoreTaskTemplateTargetDatabase(
                                           connection: String,
                                           source: String,
                                           mode: WriteMode,
                                           partitionMode: String,
                                           targetSchema: String,
                                           targetTable: String,
                                           columns: List[CoreTaskTemplateTargetColumn],
                                           uniqueKey: List[String] = List.apply(),
                                           deduplicationKey: List[String] = List.apply(),

                                           partitionBy: List[String] = List.apply(),
                                           scd:String,
                                         )