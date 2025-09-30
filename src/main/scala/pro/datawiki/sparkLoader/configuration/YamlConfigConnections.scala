package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigConnections(sourceName: String,
                                 connection: String,
                                 configLocation: String,
                                )
