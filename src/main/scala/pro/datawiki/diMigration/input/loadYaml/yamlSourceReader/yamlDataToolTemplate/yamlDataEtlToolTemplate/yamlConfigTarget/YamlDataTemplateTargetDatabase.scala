package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFramePartition, DataFrameTrait}
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.CoreTaskTemplateTargetDatabase
import pro.datawiki.sparkLoader.configuration.YamlConfigTargetTrait
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, WriteMode}
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

case class YamlDataTemplateTargetDatabase(
                                           connection: String,
                                           source: String,
                                           mode: WriteMode,
                                           partitionMode: String,
                                           targetTable: String,
                                           columns: List[YamlDataTemplateTargetColumn],
                                           uniqueKey: List[String] = List.apply(),
                                           deduplicationKey: List[String] = List.apply(),
                                           partitionBy: List[String] = List.apply(),
                                         ) {
  def getCoreTaskTemplateTargetDatabase: CoreTaskTemplateTargetDatabase = {
    return CoreTaskTemplateTargetDatabase(
      connection = connection,
      source = source,
      mode = mode,
      partitionMode = partitionMode,
      targetTable = targetTable,
      columns = columns.map(x => x.getCoreTaskTemplateTargetColumn),
      uniqueKey = uniqueKey,
      deduplicationKey = deduplicationKey,
      partitionBy = partitionBy
    )

  }
}