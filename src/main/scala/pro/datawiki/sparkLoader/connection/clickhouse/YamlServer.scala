package pro.datawiki.sparkLoader.connection.clickhouse

case class YamlServer(
                       master: YamlServerHost,
                       replica: List[YamlServerHost],
                     )
