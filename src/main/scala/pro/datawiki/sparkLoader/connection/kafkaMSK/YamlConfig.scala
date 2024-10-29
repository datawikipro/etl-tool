package pro.datawiki.sparkLoader.connection.kafkaMSK

case class YamlConfig(
                       host: String,
                       port: String,
                       accessKey: String,
                       secretAccessKey: String,
                     )