package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.configuration.{RunConfig, YamlConfigTargetTrait}
import pro.datawiki.sparkLoader.connection.{FileStorageTrait, QueryTrait}
import pro.datawiki.sparkLoader.task.Context

case class YamlDataTemplateTargetMessageBroker(
                                       connection: String,
                                       source: String,
                                       mode: String = "append",
                                       target: String,
                                       partitionMode:String
                                     ) 