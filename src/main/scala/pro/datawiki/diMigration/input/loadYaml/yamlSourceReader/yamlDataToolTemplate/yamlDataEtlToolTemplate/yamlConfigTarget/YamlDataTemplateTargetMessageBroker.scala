package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.YamlDataTemplateConnect
import pro.datawiki.sparkLoader.configuration.YamlConfigTargetTrait
import pro.datawiki.sparkLoader.connection.{FileStorageTrait, QueryTrait}


case class YamlDataTemplateTargetMessageBroker(
                                                connection: YamlDataTemplateConnect,
                                                source: String,
                                                mode: String = "append",
                                                target: String,
                                                partitionMode: String
                                              )