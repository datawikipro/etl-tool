package pro.datawiki.sparkLoader.connection.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, SparkSession}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, QueryTrait, ConnectionTrait}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions.col
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.util.Properties
import org.apache.spark.sql.avro.functions.from_avro

class LoaderKafka(configYaml: YamlConfig) extends ConnectionTrait, QueryTrait {
  override def getDataFrameFromTopic(topic: String): DataFrame = {
    val df = SparkObject.spark
      .read
      .format("kafka")
      .option("mode", "DROPMALFORMED")
      .option("kafka.bootstrap.servers", s"${configYaml.host}:${configYaml.port}")
      .option("subscribe", topic)
      //.option("subscribePattern", topic)
      //      .option("startingOffsets", "earliest")
      //      .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
      //      .option("kafka.security.protocol", "SASL_SSL")
      //      .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName=AdminDataPlatform;") //awsProfileName="platform-dev-kafka"
      //      .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
      //.option("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")
      //.option("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer")


      //.option("mode", "DROPMALFORMED")
      .load()
      df.show()
    df.printSchema()

//    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("conf/webhook.avsc")))
//    val jmap = new java.util.HashMap[String, String]()
//    jmap.put("mode", "PERMISSIVE")
//    val personDF = df.select(from_avro(col("value"), jsonFormatSchema, jmap) as "value").select("value.*")
//    val jmap = new java.util.HashMap[String, String]()
//    val personDF = df.select(from_avro(col("value"), jsonFormatSchema, jmap) as "value").select("value.*")
//    personDF.printSchema()
//    personDF.show()
//    personDF.write.format("avro").save("path/asd")

//
//    val query = personDF.
//      writeStream.trigger(Trigger.ProcessingTime("5 seconds")).
//      format("json") // can be "orc", "json", "csv", etc.
//      .option("checkpointLocation", "path/checkpoint/")
//      .option("path", "path/json/")
//      .start()

    //query.awaitTermination()
    throw Exception()
    return df
  }
}

object LoaderKafka {
  def apply(inConfig: String): LoaderKafka = {

    val lines: String = Files.readString(Paths.get(inConfig))

    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val configYaml: YamlConfig = mapper.readValue(lines, classOf[YamlConfig])
    return new LoaderKafka(configYaml)
  }
}
