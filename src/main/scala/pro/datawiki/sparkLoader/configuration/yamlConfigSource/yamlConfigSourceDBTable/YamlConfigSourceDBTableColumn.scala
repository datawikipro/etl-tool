package pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigSourceDBTableColumn(columnName: String,
                                         columnType: String = null
                                        )
