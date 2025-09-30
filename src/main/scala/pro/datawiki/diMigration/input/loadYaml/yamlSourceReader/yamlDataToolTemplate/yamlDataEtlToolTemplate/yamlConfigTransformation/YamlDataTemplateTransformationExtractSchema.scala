package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation

case class YamlDataTemplateTransformationExtractSchema(
                                                        tableName: String,
                                                        jsonColumn: String,
                                                        jsonResultColumn: String,
                                                        baseSchema: String
                                                      )