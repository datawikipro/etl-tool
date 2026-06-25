package pro.datawiki.sparkLoader.connection.minIo.minioIceberg

import com.fasterxml.jackson.annotation.JsonProperty

case class YamlConfigRegister(
  @JsonProperty("type") registerType: String,
  url: Option[String] = None,
  user: Option[String] = None
)
