package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import pro.datawiki.sparkLoader.connection.WriteMode

case class YamlConfigTargetColumn(columnName: String,
                                  isNewCCD: Boolean,
                                  domainName: String,
                                  tenantName: String,
                                  isNullable: Boolean
                                 )