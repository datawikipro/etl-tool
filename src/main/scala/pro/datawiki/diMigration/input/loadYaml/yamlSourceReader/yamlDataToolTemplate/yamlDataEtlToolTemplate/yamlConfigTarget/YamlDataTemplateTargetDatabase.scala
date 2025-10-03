package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFramePartition, DataFrameTrait}
import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.CoreTaskTemplateTargetDatabase
import pro.datawiki.sparkLoader.configuration.YamlConfigTargetTrait
import pro.datawiki.sparkLoader.connection.DatabaseTrait
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

case class YamlDataTemplateTargetDatabase(
                                           connection: String,
                                           source: String,
                                           mode: WriteMode,
                                           partitionMode: String,
                                           targetSchema: String,
                                           targetTable: String,
                                           columns: List[YamlDataTemplateTargetColumn],
                                           uniqueKey: List[String],
                                           deduplicationKey: List[String],
                                           partitionBy: List[String],
                                           scd:String
                                         ) 