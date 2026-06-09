package pro.datawiki.sparkLoader.connection.kafka.kafkaTemplate

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*
import pro.datawiki.sparkLoader.SparkObject
import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.kafka.kafkaTemplate.LoaderKafkaTemplateBatch
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, KafkaMetadataTrait, QueryTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.ConnectionEnum
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.yamlConfiguration.YamlClass
import org.apache.spark.sql.functions._


class LoaderKafkaTemplateBatch(`bootstrap.servers`: List[String],
                               offestConnector: String,
                               offsetMetatable: String) extends LoaderKafkaTemplateBase(`bootstrap.servers`) {
  def getDataFrameFromTopic(topic: String, startingOffsets: Option[String] = None): DataFrame = {

    var dataStreamReader = SparkObject.spark.read.format("kafka").option("mode", "DROPMALFORMED").option("subscribe", topic)

    configs.foreach(i => dataStreamReader = dataStreamReader.option(s"kafka.${i._1}", i._2))

    dataStreamReader = dataStreamReader.option("maxOffsetsPerTrigger", "10000")
    dataStreamReader = dataStreamReader.option("startingOffsets", startingOffsets.getOrElse("earliest"))
    dataStreamReader = dataStreamReader.option("failOnDataLoss", "true")
    dataStreamReader = dataStreamReader.option("group.id", "scala-etl-tool")
    dataStreamReader = dataStreamReader.option("enable.auto.commit", "false")

    val df = dataStreamReader.load().selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset", "timestamp", "timestampType")

    val maxOffsets = df
      .groupBy("topic", "partition")
      .agg(max("offset").as("max_offset"))
      .collect()

    // Convert to Map[partition, nextOffset]
    maxOffsets.foreach { row => modifyOffset += ((row.getAs[String]("topic"), row.getAs[Int]("partition")) -> (row.getAs[Long]("max_offset") + 1)) }

    return df
  }
  
  // Get metadata connection that supports Kafka offset operations
  private def getOffsetConnection: KafkaMetadataTrait = {
    ConnectionTrait.apply(null, offestConnector, offestConnector) match {
      case x: KafkaMetadataTrait => x
      case _ => throw new IllegalArgumentException("Offset connector must implement KafkaMetadataTrait")
    }
  }

  def readOffsets(topic: String): Option[String] = {
    try {
      val metadataConn = getOffsetConnection
      metadataConn.readKafkaOffsets(offsetMetatable, topic)
    } catch {
      case e: Exception =>
        logError(s"Failed to read offsets for topic: $topic", e)
        None
    }
  }
  
  override def close():Unit={
    //TODO сделать как будет время
//    val metadataConn = getOffsetConnection
//    metadataConn.updateKafkaOffsets(offsetMetatable, modifyOffset)
    super.close()
  }
  
  var modifyOffset: Map[(String, Int), Long] = Map.empty

}
