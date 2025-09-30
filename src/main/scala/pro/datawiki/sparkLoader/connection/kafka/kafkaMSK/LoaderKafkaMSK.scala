package pro.datawiki.sparkLoader.connection.kafka.kafkaMSK

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.kafka.kafkaBase
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}
import pro.datawiki.yamlConfiguration.YamlClass

class LoaderKafkaMSK(configYaml: YamlConfig) extends kafkaBase.LoaderKafka(kafkaBase.YamlConfig(`bootstrap.servers` = configYaml.`bootstrap.servers`)), ConnectionTrait, QueryTrait {

  configs += ("kafka.sasl.mechanism", "AWS_MSK_IAM")
  configs += ("kafka.security.protocol", "SASL_SSL")
  configs += ("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName=AdminDataPlatform;") //awsProfileName="platform-dev-kafka"
  configs += ("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")

}

object LoaderKafkaMSK extends YamlClass {
  def apply(inConfig: String): LoaderKafkaMSK = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderKafkaMSK(configYaml)
  }
}