package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

case class YamlConfigTargetColumn(columnName: String,
                                  isNewCCD: Boolean,
                                  domainName: String,
                                  tenantName: String
                                 )
