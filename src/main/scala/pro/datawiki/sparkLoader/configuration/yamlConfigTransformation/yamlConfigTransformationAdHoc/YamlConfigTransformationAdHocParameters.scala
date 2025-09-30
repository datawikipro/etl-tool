package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationAdHoc

case class YamlConfigTransformationAdHocParameters(
                                                    parameterName: String,
                                                    columnName: String = "",
                                                    default: String = "",
                                                    decode: String = "None"
                                                  ) {

}