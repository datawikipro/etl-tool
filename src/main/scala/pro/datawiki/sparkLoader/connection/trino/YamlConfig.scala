package pro.datawiki.sparkLoader.connection.trino

import com.fasterxml.jackson.annotation.JsonProperty

case class YamlConfig(
  url: String,
  user: String
)
