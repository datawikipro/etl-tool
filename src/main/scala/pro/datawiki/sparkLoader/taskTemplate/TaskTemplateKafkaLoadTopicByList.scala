package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.{DataFrameStream, DataFrameTrait}
import pro.datawiki.exception.{NotImplementedException, UnsupportedOperationException}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.traits.LoggingTrait

import scala.collection.mutable

class TaskTemplateKafkaLoadTopicByList(topicList: List[String], source: ConnectionTrait) extends TaskTemplate with LoggingTrait {
  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val startTime = logOperationStart("Kafka topic load by list", s"topics: ${topicList.mkString(",")}")

    try {
      logInfo(s"Loading data from Kafka topics: ${topicList.mkString(",")}")
      logConfigInfo("Kafka", s"topic count: ${topicList.length}, sync: $isSync")

      source match
        case x: QueryTrait => {
          logInfo("Using QueryTrait connection for Kafka")
          val dfStream = x.getDataFrameBatchFromTopic(topicList.mkString(","))
          logInfo(s"Created DataFrameStream from Kafka topics: ${topicList.mkString(",")}")

          logOperationEnd("Kafka topic load by list", startTime, s"topics: ${topicList.mkString(",")}")
          return List.apply(DataFrameStream(dfStream))
        }
        case _ =>
          logError("Kafka topic load", UnsupportedOperationException(s"Unsupported connection type for Kafka topic loading: ${source.getClass.getSimpleName}", null))
          throw UnsupportedOperationException(s"Unsupported connection type for Kafka topic loading: ${source.getClass.getSimpleName}")

    } catch {
      case e: Exception =>
        logError("Kafka topic load by list", e, s"topics: ${topicList.mkString(",")}")
        throw e
    }
  }
}
