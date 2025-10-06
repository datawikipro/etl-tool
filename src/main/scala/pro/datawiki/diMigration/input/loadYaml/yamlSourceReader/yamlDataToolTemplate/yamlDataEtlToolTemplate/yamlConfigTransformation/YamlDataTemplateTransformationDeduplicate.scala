package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation

case class YamlDataTemplateTransformationDeduplicate(
                                                      sourceTable: String,
                                                      uniqueKey: List[String],
                                                      deduplicationKey: List[String]
                                                    )
