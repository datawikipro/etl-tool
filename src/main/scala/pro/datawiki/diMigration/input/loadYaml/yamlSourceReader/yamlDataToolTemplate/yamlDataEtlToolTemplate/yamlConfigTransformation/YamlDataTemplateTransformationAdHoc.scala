package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation

import org.apache.spark.sql.Row
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.task.*

class YamlDataTemplateTransformationAdHoc(
                                           sourceObjectName: String,
                                           templateName: String,
                                           columnId: List[String] = List.apply(),
                                           asyncNumber: Int
                                         )