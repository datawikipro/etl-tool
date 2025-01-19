package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap

case class YamlConfigTransformationIdmapTemplate(
                                                  domainName: String,
                                                  isGenerated: Boolean,
                                                  rkKey: String,
                                                  systemCode: String,
                                                  columnNames: List[String]
                                                )
