package pro.datawiki.sparkLoader.connection.mongodb

case class YamlServer(
                       master: YamlServerHost,
                       replica: List[YamlServerHost] = List.apply(),
                     )
