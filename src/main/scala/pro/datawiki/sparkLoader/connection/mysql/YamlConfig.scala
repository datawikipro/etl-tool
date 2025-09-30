package pro.datawiki.sparkLoader.connection.mysql

case class YamlConfig(server: YamlServer,
                      gateway: YamlGateway,
                      login: String,
                      password: String,
                     )