package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.yamlConfigTransformationAdHoc

case class YamlDataTemplateTransformationAdHocParameters(
                                                          parameterName: String,
                                                          columnName: String = "",
                                                          default: String = "",
                                                          decode: String = "None"
                                                        ) {

}