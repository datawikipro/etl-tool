package pro.datawiki.sparkLoader.connection.mysql

case class YamlServer(
                       master: YamlServerHost,
                       replica: List[YamlServerHost],
                     )
