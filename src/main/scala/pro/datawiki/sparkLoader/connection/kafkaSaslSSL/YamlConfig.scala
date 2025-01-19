package pro.datawiki.sparkLoader.connection.kafkaSaslSSL

class YamlConfig(
                  subscribe: String,
                  `bootstrap.servers`: List[String],
                  `sasl.mechanism`: String,
                  `sasl.jaas.config`: String,
                  `ssl.client.authd`: String,
                  `ssl.truststore.type`: String,
                  `ssl.truststore.certificates`: String,
                  `ssl.endpoint.identification.algorithm`: String
                ) {
  def getBootstrapServers: List[String] = {
    return `bootstrap.servers`
  }

  def getSslTruststoreType: String = {
    return `ssl.truststore.type`
  }

  def getSslTruststoreCertificates: String = {
    return `ssl.truststore.certificates`
  }
}