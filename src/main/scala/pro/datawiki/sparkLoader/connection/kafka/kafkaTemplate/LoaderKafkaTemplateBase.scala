package pro.datawiki.sparkLoader.connection.kafka.kafkaTemplate

import org.apache.kafka.clients.admin.{AdminClient, DescribeClusterOptions, ListTopicsOptions, NewTopic}
import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.ConfigurationException
import pro.datawiki.schemaValidator.*
import pro.datawiki.schemaValidator.baseSchema.{BaseSchemaObject, BaseSchemaObjectTemplate, BaseSchemaString, BaseSchemaStringTemplate}
import pro.datawiki.schemaValidator.spark.*
import pro.datawiki.sparkLoader.traits.LoggingTrait

import java.net.{InetSocketAddress, Socket}
import java.util
import java.util.Properties
import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}


class LoaderKafkaTemplateBase(`bootstrap.servers`: List[String]) extends LoggingTrait{
  logInfo("Creating Kafka connection")
  var configs: mutable.Map[String, String] = mutable.Map()
  configs += ("bootstrap.servers", `bootstrap.servers`.mkString(","))
  configs += ("request.timeout.ms", s"${5 * 60 * 1000}")
  configs += ("default.api.timeout.ms", s"${5 * 60 * 1000}")

  // Configurable timeouts (in milliseconds)
  private val connectivityTimeoutMs: Int = 5000
  private val clusterHealthTimeoutMs: Int = 10000
  private val listTopicsTimeoutMs: Int = 30000

  private var locAdminClient: AdminClient = null

  def adminClient: AdminClient = {
    if locAdminClient == null then {
      val props = new Properties()
      configs.foreach(i => props.put(i._1, i._2))
      locAdminClient = AdminClient.create(props)
    }
    return locAdminClient
  }

  /**
   * Check if at least one Kafka broker is reachable via TCP socket.
   * This is a fast pre-check before attempting Kafka operations.
   */
  private def checkBrokerConnectivity(): Either[String, String] = {
    val brokers = `bootstrap.servers`
    val errors = mutable.ListBuffer[String]()
    
    for (broker <- brokers) {
      Try {
        val parts = broker.split(":")
        val host = parts(0)
        val port = if (parts.length > 1) parts(1).toInt else 9092
        
        val socket = new Socket()
        try {
          socket.connect(new InetSocketAddress(host, port), connectivityTimeoutMs)
          logInfo(s"Successfully connected to broker: $broker")
          return Right(broker)
        } finally {
          socket.close()
        }
      } match {
        case Success(_) => // Already returned
        case Failure(e) => 
          val errMsg = s"$broker: ${e.getClass.getSimpleName} - ${e.getMessage}"
          errors += errMsg
          logWarning(s"Failed to connect to broker $broker: ${e.getMessage}")
      }
    }
    
    Left(s"Cannot connect to any Kafka broker. Tried: ${brokers.mkString(", ")}. Errors: ${errors.mkString("; ")}")
  }

  /**
   * Check Kafka cluster health by describing the cluster.
   * Returns cluster info if healthy, or error message if not.
   */
  private def checkClusterHealth(): Either[String, ClusterHealthInfo] = {
    val describeOptions = new DescribeClusterOptions().timeoutMs(clusterHealthTimeoutMs)
    
    Try {
      val clusterResult = adminClient.describeCluster(describeOptions)
      
      val controller = clusterResult.controller().get(clusterHealthTimeoutMs, TimeUnit.MILLISECONDS)
      val nodes = clusterResult.nodes().get(clusterHealthTimeoutMs, TimeUnit.MILLISECONDS)
      val clusterId = clusterResult.clusterId().get(clusterHealthTimeoutMs, TimeUnit.MILLISECONDS)
      
      val controllerInfo = if (controller != null) s"id=${controller.id()}, host=${controller.host()}" else "none"
      logInfo(s"Cluster health check passed: clusterId=$clusterId, nodes=${nodes.size()}, controller=$controllerInfo")
      
      ClusterHealthInfo(
        clusterId = clusterId,
        nodeCount = nodes.size(),
        controllerId = Option(controller).map(_.id()),
        controllerHost = Option(controller).map(_.host())
      )
    } match {
      case Success(info) => Right(info)
      case Failure(e: TimeoutException) =>
        Left(s"Cluster health check timed out after ${clusterHealthTimeoutMs}ms. The Kafka cluster may be unavailable or overloaded.")
      case Failure(e) =>
        Left(s"Cluster health check failed: ${e.getClass.getSimpleName} - ${e.getMessage}")
    }
  }

  def createTopic(name: String, numPartitions: Int = 1, replicationFactor: Short = 1): Boolean = {
    val newTopic: NewTopic = new NewTopic(name, numPartitions, replicationFactor)
    val newTopics: List[NewTopic] = List.apply(newTopic)
    val result = adminClient.createTopics(newTopics.asJava)
    throw ConfigurationException("Метод в LoaderKafka еще не реализован")
  }

  def getListTopics(template: String): DataFrame = {
    // Step 1: Check broker connectivity (fast TCP check)
    checkBrokerConnectivity() match {
      case Left(error) =>
        logWarning(s"Kafka connectivity check failed: $error")
        throw new KafkaConnectionException(error)
      case Right(broker) =>
        logInfo(s"Broker connectivity confirmed via: $broker")
    }
    
    // Step 2: Check cluster health
    val clusterInfo = checkClusterHealth() match {
      case Left(error) =>
        logWarning(s"Kafka cluster health check failed: $error")
        throw new KafkaClusterUnhealthyException(error)
      case Right(info) => info
    }
    
    // Step 3: List topics with explicit timeout
    logInfo(s"Listing topics with timeout ${listTopicsTimeoutMs}ms...")
    val listTopicsOptions = new ListTopicsOptions()
      .listInternal(true)
      .timeoutMs(listTopicsTimeoutMs)
    
    val topics = Try {
      adminClient.listTopics(listTopicsOptions).names().get(listTopicsTimeoutMs, TimeUnit.MILLISECONDS)
    } match {
      case Success(t) => t
      case Failure(e: TimeoutException) =>
        val msg = s"List topics operation timed out after ${listTopicsTimeoutMs}ms. " +
          s"Cluster info: ${clusterInfo.nodeCount} nodes, controller: ${clusterInfo.controllerId.getOrElse("unknown")}"
        logWarning(msg)
        throw new KafkaOperationTimeoutException(msg)
      case Failure(e) =>
        val msg = s"List topics failed: ${e.getClass.getSimpleName} - ${e.getMessage}"
        logWarning(msg)
        throw ConfigurationException(msg, e)
    }
    
    logInfo(s"Successfully retrieved ${topics.size()} topics from Kafka cluster")

    val list: List[BaseSchemaObject] =
      topics.asScala.map(i =>
        BaseSchemaObject(List(("topic_name", BaseSchemaString(i, false))), false)
      ).toList
    implicit val spark: org.apache.spark.sql.SparkSession = pro.datawiki.sparkLoader.SparkObject.spark
    return SparkConstructor.getDataFrameFromListBaseSchemaObjects(
      list,
      BaseSchemaObjectTemplate(List(("topic_name", BaseSchemaStringTemplate(false))), false))
  }
  
  def close(): Unit = {
    if locAdminClient != null then locAdminClient.close()
  } 
}
