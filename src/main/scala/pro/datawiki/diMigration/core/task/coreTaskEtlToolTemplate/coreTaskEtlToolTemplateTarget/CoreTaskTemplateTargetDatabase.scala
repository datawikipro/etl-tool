package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget

import org.apache.spark.sql.functions.*
import pro.datawiki.sparkLoader.connection.WriteMode

case class CoreTaskTemplateTargetDatabase(
                                     connection: String,
                                     source: String,
                                     mode: WriteMode,
                                     partitionMode: String,

                                     targetTable: String,
                                     columns: List[CoreTaskTemplateTargetColumn],
                                     uniqueKey: List[String] = List.apply(),
                                     deduplicationKey: List[String] = List.apply(),

                                     partitionBy: List[String] = List.apply(),
                                   ) 