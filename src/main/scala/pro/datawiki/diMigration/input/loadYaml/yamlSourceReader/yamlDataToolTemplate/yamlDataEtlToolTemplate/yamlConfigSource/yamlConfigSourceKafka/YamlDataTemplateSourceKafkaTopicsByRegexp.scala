package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource.yamlConfigSourceKafka

import org.apache.spark.sql.Row
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait, QueryTrait}
import pro.datawiki.sparkLoader.task.*

class YamlDataTemplateSourceKafkaTopicsByRegexp(template: String) 