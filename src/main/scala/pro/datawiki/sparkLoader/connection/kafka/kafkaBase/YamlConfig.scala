package pro.datawiki.sparkLoader.connection.kafka.kafkaBase

case class YamlConfig(
                       `bootstrap.servers`: List[String],
                       checkpointLocation: Option[String] = None
                     )