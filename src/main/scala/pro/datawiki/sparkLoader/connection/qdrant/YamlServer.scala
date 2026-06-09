package pro.datawiki.sparkLoader.connection.qdrant

case class YamlServer(
                       master: YamlServerHost,
                       replica: List[YamlServerHost] = List.apply()
                     )
