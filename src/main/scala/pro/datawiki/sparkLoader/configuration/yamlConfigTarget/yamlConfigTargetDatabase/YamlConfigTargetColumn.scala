package pro.datawiki.sparkLoader.configuration.yamlConfigTarget.yamlConfigTargetDatabase

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTargetColumn(columnName: String,
                                  @JsonIgnore
                                  isNullable: Boolean,
                                  columnType: String,
                                  columnTypeDecode: Boolean
                                 )