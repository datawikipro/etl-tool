package pro.datawiki.sparkLoader.connection.mssql

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
case class YamlConfig(
  host: String,
  port: Int = 1433,
  database: String,
  user: String = null,
  login: String = null,
  password: String,
  driver: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  options: Map[String, String] = Map.empty
)
