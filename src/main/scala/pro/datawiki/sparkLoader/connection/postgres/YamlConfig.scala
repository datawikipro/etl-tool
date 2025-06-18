package pro.datawiki.sparkLoader.connection.postgres

case class YamlConfig(server: YamlServer,
                      login: String,
                      password: String,
                     )