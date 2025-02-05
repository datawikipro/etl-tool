package pro.datawiki.sparkLoader.connection.kafka

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.avro.*
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.{LogMode, SparkObject, YamlClass}

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
    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }

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

object LoaderKafka  extends YamlClass {
  def apply(inConfig: String): LoaderKafka = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderKafka(configYaml)
  }
}
