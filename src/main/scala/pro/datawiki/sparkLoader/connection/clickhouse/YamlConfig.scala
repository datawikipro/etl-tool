package pro.datawiki.sparkLoader.connection.clickhouse

case class YamlConfig(server: YamlServer,
                      login: String,
                      password: String,
                     )