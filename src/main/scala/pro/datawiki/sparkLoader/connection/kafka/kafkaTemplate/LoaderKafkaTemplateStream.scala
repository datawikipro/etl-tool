package pro.datawiki.sparkLoader.connection.kafka.kafkaTemplate

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject

class LoaderKafkaTemplateStream(`bootstrap.servers`: List[String]) extends LoaderKafkaTemplateBase(`bootstrap.servers`) {
  def getDataFrameFromTopic(topic: String): DataFrame = {
    var dataStreamReader = SparkObject.spark.readStream.format("kafka").option("mode", "DROPMALFORMED").option("subscribe", topic)

    configs.foreach(i => dataStreamReader = dataStreamReader.option(s"kafka.${i._1}", i._2))

    dataStreamReader = dataStreamReader.option("maxOffsetsPerTrigger", "10000")
    dataStreamReader = dataStreamReader.option("startingOffsets", "earliest")
    dataStreamReader = dataStreamReader.option("failOnDataLoss", "true")
    dataStreamReader = dataStreamReader.option("group.id", "scala-etl-tool")
    dataStreamReader = dataStreamReader.option("enable.auto.commit", "false")

    val df = dataStreamReader.load().selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset", "timestamp", "timestampType")
    return df
  }
}
