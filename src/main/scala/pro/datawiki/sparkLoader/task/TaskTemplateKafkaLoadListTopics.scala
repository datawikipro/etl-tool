package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}

import scala.collection.mutable

class TaskTemplateKafkaLoadListTopics(template: String, source: ConnectionTrait) extends TaskTemplate {
  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    source match
      case x: QueryTrait => {
        return List.apply(DataFrameOriginal(x.getListTopics(template)))
      }
      case _ => throw Exception()

    throw Exception()
  }
}

