package pro.datawiki.sparkLoader.connection.kafka.kafkaSaslSSL

case class YamlConfig(
                       subscribe: String,
                       `bootstrap.servers`: List[String],
                       `sasl.mechanism`: String,
                       `sasl.jaas.config`: String,
                       `ssl.client.authd`: String,
                       `ssl.truststore.type`: String,
                       `ssl.truststore.certificates`: String,
                       `ssl.endpoint.identification.algorithm`: String
                     )