package pro.datawiki.sparkLoader.connection.kafka.kafkaBase

import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions, NewTopic}
import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.ConfigurationException
import pro.datawiki.schemaValidator.*
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaObjectTemplate, BaseSchemaString, BaseSchemaStringTemplate}
import pro.datawiki.schemaValidator.spark.*
import pro.datawiki.schemaValidator.spark.sparkType.{SparkRowElementList, SparkRowElementRow, SparkRowElementString}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.ConnectionEnum
import pro.datawiki.yamlConfiguration.YamlClass

import java.util
import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters.*


class LoaderKafka(configYaml: YamlConfig, configLocation: String) extends ConnectionTrait, QueryTrait {
  private val _configLocation: String = configLocation
  
  logInfo("Creating Kafka connection")
  var configs: mutable.Map[String, String] = mutable.Map()
  configs += ("bootstrap.servers", configYaml.`bootstrap.servers`.mkString(","))
  configs += ("request.timeout.ms", s"${5 * 60 * 1000}")
  configs += ("default.api.timeout.ms", s"${5 * 60 * 1000}")

  // Получить путь для checkpoint location
  def getCheckpointLocation(topic: String): String = {
    configYaml.checkpointLocation.getOrElse(s"/opt/etl-tool/kafka/checkpoints/$topic/")
  }

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

    
    val list: List[BaseSchemaObject] = 
      topics.asScala.map(i =>
        BaseSchemaObject(List(("topic_name", BaseSchemaString(i,false))), false)
      ).toList
    return SparkConstructor.getDataFrameFromListBaseSchemaObjects(
      list,
      BaseSchemaObjectTemplate(List(("topic_name", BaseSchemaStringTemplate(false))), false))
  }

  override def getDataFrameBatchFromTopic(topic: String): DataFrame = {
    var dataStreamReader = SparkObject.spark
      .readStream
      .format("kafka")
      .option("mode", "DROPMALFORMED")
      .option("subscribe", topic)

    configs.foreach(i => dataStreamReader = dataStreamReader.option(s"kafka.${i._1}", i._2))

    dataStreamReader = dataStreamReader.option("maxOffsetsPerTrigger", "10000")
    dataStreamReader = dataStreamReader.option("startingOffsets", "earliest")
    dataStreamReader = dataStreamReader.option("failOnDataLoss", "true") // Исправлено для гарантии целостности данных
    dataStreamReader = dataStreamReader.option("group.id", "scala-etl-tool")
    dataStreamReader = dataStreamReader.option("enable.auto.commit", "false")
    // УБРАНА неправильная настройка trigger - она должна быть в writeStream, а не readStream

    val df = dataStreamReader.load().selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset", "timestamp", "timestampType")
    return df
  }

  override def createTopic(name: String, numPartitions: Int = 1, replicationFactor: Short = 1): Boolean = {
    val newTopic: NewTopic = new NewTopic(name, numPartitions, replicationFactor)
    val newTopics: List[NewTopic] = List.apply(newTopic)
    val result = adminClient.createTopics(newTopics.asJava)
    throw ConfigurationException("Метод в LoaderKafka еще не реализован")
  }

  override def close(): Unit = {
    if locAdminClient != null then locAdminClient.close()
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = {
    ConnectionEnum.kafka
  }

  override def getConfigLocation(): String = {
    _configLocation
  }
}

object LoaderKafka extends YamlClass {
  def apply(inConfig: String): LoaderKafka = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderKafka(configYaml, inConfig)
  }
}
