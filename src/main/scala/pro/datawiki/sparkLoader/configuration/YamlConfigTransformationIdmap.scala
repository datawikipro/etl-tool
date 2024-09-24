package pro.datawiki.sparkLoader.configuration

case class YamlConfigTransformationIdmap(
                                          sourceName: String,
                                          tenantName: String,
                                          idmaps: List[YamlConfigTransformationIdmapTemplate]
                                        )
