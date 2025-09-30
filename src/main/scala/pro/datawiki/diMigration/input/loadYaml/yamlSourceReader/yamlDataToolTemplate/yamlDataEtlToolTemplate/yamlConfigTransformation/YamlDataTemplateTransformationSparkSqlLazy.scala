package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation

import pro.datawiki.sparkLoader.SparkObject

case class YamlDataTemplateTransformationSparkSqlLazy(
                                                   sql: String,
                                                   lazyTable: List[String] = List.empty
                                                 )