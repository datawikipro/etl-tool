package pro.datawiki.sparkLoader.connection.postgres

case class YamlServer(
                       master: YamlServerHost,
                       replica: List[YamlServerHost] = List.apply(),
                     )
