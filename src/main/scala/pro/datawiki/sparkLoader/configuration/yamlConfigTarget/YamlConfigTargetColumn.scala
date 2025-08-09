package pro.datawiki.sparkLoader.configuration.yamlConfigTarget

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTargetColumn(columnName: String,
                                  @JsonIgnore
                                  isNewCCD: Boolean,
                                  @JsonIgnore
                                  domainName: String,
                                  @JsonIgnore
                                  tenantName: String,
                                  @JsonIgnore
                                  isNullable: Boolean
                                 )