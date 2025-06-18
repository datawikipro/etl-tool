package pro.datawiki.sparkLoader.connection.kafka.kafkaMSK

case class YamlConfig(
                       `bootstrap.servers`: List[String],
                       accessKey: String,
                       secretAccessKey: String,
                     )