package pro.datawiki.sparkLoader.connection.kafka.kafkaBase

import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions, NewTopic}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.DataStreamReader
import pro.datawiki.exception.ConfigurationException
import pro.datawiki.schemaValidator.*
import pro.datawiki.schemaValidator.sparkRow.*
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}
import pro.datawiki.yamlConfiguration.YamlClass

import java.util
import java.util.{Collection, Properties}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*


class LoaderKafka(configYaml: YamlConfig) extends ConnectionTrait, QueryTrait {
  var configs: mutable.Map[String, String] = mutable.Map()
  configs += ("bootstrap.servers", configYaml.`bootstrap.servers`.mkString(","))
  configs += ("request.timeout.ms", s"${5 * 60 * 1000}")
  configs += ("default.api.timeout.ms", s"${5 * 60 * 1000}")

  private var locAdminClient: AdminClient = null

  private def adminClient: AdminClient = {
    if locAdminClient == null then {
      val props = new Properties()
      configs.foreach(i => props.put(i._1, i._2))
      locAdminClient = AdminClient.create(props)
    }
    return locAdminClient
  }

  override def getListTopics(template: String): DataFrame = {
    val listTopicsOptions = new ListTopicsOptions().listInternal(true)
    val topics = adminClient.listTopics(listTopicsOptions).names().get()

    var attributes: List[SparkRowAttribute] = List.apply()

    var list: List[SparkRowElementRow] = List.apply()
    topics.asScala.foreach(i => {
      list = list.appended(SparkRowElementRow(List.apply(SparkRowAttribute("topic_name", SparkRowElementString(i)))))
    })
    attributes = attributes.appended(SparkRowAttribute("list", SparkRowElementList(list)))
    val sparkRow: SparkRow = SparkRow(attributes)
    return sparkRow.getDataFrame

  }

  override def getDataFrameBatchFromTopic(topic: String): DataFrame = {
    var dataStreamReader = SparkObject.spark
      .readStream
      .format("kafka")
      .option("mode", "DROPMALFORMED")
      .option("subscribe", topic)

    configs.foreach(i => dataStreamReader = dataStreamReader.option(s"kafka.${i._1}", i._2))

    dataStreamReader = dataStreamReader.option("maxOffsetsPerTrigger", "2000")
    dataStreamReader = dataStreamReader.option("startingOffsets", "earliest")
    dataStreamReader = dataStreamReader.option("failOnDataLoss", "false") //TODO Сделать параметром
    dataStreamReader = dataStreamReader.option("group.id", "scala-etl-tool")
    dataStreamReader = dataStreamReader.option("enable.auto.commit", "false")
    dataStreamReader = dataStreamReader.option("trigger", "10 seconds")

    val df = dataStreamReader.load().selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset", "timestamp", "timestampType")
    return df
  }

  override def createTopic(name: String, numPartitions: Int = 1, replicationFactor: Short = 1): Boolean = {
    val newTopic:NewTopic = new NewTopic(name, numPartitions, replicationFactor)
    val newTopics: List[NewTopic] = List.apply(newTopic)
    val result = adminClient.createTopics(newTopics.asJava)
    throw new ConfigurationException("Метод в LoaderKafka еще не реализован")
  }

  override def close(): Unit = {
    if locAdminClient != null then locAdminClient.close()
  }
}

object LoaderKafka extends YamlClass {
  def apply(inConfig: String): LoaderKafka = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderKafka(configYaml)
  }
}
