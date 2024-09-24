package pro.datawiki.sparkLoader.configuration

case class YamlConfigTransformationIdmapTemplate(
                                                  domainName: String,
                                                  isGenerated: Boolean,
                                                  columnNames: List[String]
                                                )
