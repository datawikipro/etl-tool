package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode
import pro.datawiki.sparkLoader.task.Task

class YamlDataTemplateTargetBase(connection: String,
                                 mode: String = "append",
                                 partitionMode: String,
                                 source: String
                                )