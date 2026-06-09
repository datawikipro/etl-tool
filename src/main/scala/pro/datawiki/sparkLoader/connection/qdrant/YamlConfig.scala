package pro.datawiki.sparkLoader.connection.qdrant

case class YamlConfig(
                       server: YamlServer,
                       apiKey: String = null,
                       readOnlyApiKey: String = null
                     )
