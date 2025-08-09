package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation

import pro.datawiki.sparkLoader.SparkObject

case class YamlDataTemplateTransformationSparkSql(
                                             sql: String,
                                             isLazyTransform: Boolean = false,
                                             lazyTable: List[String] = List.empty
                                           ) 