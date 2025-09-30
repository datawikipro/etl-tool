package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation

import pro.datawiki.sparkLoader.SparkObject

case class YamlDataTemplateTransformationSparkDeduplicate(tableName: String,
                                                          uniqueKey: List[String],
                                                          deduplicateColumn: List[String])