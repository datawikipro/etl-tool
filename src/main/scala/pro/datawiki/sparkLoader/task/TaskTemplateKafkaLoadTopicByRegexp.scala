package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import scala.collection.mutable

class TaskTemplateKafkaLoadTopicByRegexp(template: String, source: ConnectionTrait) extends TaskTemplate {
  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    throw Exception()
  }
}
