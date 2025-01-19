package pro.datawiki.sparkLoader.connection.kafkaMSK

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.kafka.LoaderKafka.getLines
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.{SparkObject, YamlClass}

class LoaderKafkaMSK(configYaml: YamlConfig) extends ConnectionTrait,QueryTrait {

  override def getDataFrameFromTopic(topic:String):DataFrame ={
    SparkObject.spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"${configYaml.host}:${configYaml.port}")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName=AdminDataPlatform;") //awsProfileName="platform-dev-kafka"
      .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
      .option("kafka.sasl.aws.accessKeyId", configYaml.accessKey)
      .option("kafka.sasl.aws.secretAccessKey", configYaml.secretAccessKey)
      .load()
  }
}

object LoaderKafkaMSK  extends YamlClass {
  def apply(inConfig:String):LoaderKafkaMSK = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderKafkaMSK(configYaml)
  }
}