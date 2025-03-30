package pro.datawiki.sparkLoader.connection.kafkaSaslSSL

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.{LogMode, SparkObject, YamlClass}

class LoaderKafkaSaslSSL(configYaml: YamlConfig) extends ConnectionTrait, QueryTrait {
  override def getDataFrameBatchFromTopic(topic: String): DataFrame = {
    val df = SparkObject.spark
      .read
      .format("kafka")
      .option("mode", "DROPMALFORMED")
      .option("kafka.bootstrap.servers", configYaml.getBootstrapServers.mkString(","))
      .option("subscribe", topic)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
      .option("kafka.ssl.client.authd", "required")
      .option("kafka.ssl.endpoint.identification.algorithm", "")
      .option("kafka.startingOffsets", "earliest")
      .option("kafka.ssl.truststore.type", configYaml.getSslTruststoreType)
      .option("kafka.ssl.truststore.certificates", configYaml.getSslTruststoreCertificates)
      .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.scram.ScramLoginModule required username='user1' password='ooGh1eegue3ieb1aeth2' serviceName='kafka';""")
      .load()

    val df2 = df.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset", "timestamp", "timestampType")
    LogMode.debugDF(df2)
    return df2
  }
}

object LoaderKafkaSaslSSL extends YamlClass {
  def apply(inConfig: String): LoaderKafkaSaslSSL = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderKafkaSaslSSL(configYaml)
  }
}
