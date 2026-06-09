package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.traits.LoggingTrait

import scala.collection.mutable

class TaskTemplateKafkaLoadListTopics(template: String, source: ConnectionTrait) extends TaskTemplate with LoggingTrait {
  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val startTime = logOperationStart("Kafka list topics", s"template: $template")

    try {
      logInfo(s"Loading Kafka topics list with template: $template")
      logConfigInfo("Kafka list topics", s"template: $template, sync: $isSync")

      source match
        case x: QueryTrait => {
          logInfo("Using QueryTrait connection for Kafka topics list")
          val df = x.getListTopics(template)

          logOperationEnd("Kafka list topics", startTime, s"template: $template")
          return List.apply(DataFrameOriginal(df))
        }
        case _ =>
          logError("Kafka list topics", pro.datawiki.exception.UnsupportedOperationException("Unsupported connection type for Kafka topics"))
          throw UnsupportedOperationException("Unsupported connection type for Kafka topics")

    } catch {
      case e: Exception =>
        logError("Kafka list topics", e, s"template: $template")
        throw e
    }
  }
}

