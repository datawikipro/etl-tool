package pro.datawiki.sparkLoader.connection.kafkaMSK

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, SparkSession}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, ConnectionTrait,QueryTrait}

import java.nio.file.{Files, Paths}
import java.util.Properties

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

object LoaderKafkaMSK{
  def apply(inConfig:String):LoaderKafkaMSK = {

    val lines: String = Files.readString(Paths.get(inConfig))

    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val configYaml: YamlConfig = mapper.readValue(lines, classOf[YamlConfig])
    return new LoaderKafkaMSK(configYaml)
  }
}