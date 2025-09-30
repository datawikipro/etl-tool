package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import scala.collection.mutable

class TaskTemplateKafkaLoadTopicByRegexp(template: String, source: ConnectionTrait) extends TaskTemplate {
  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    throw NotImplementedException("Kafka topic loading by regexp not implemented")
  }
}
