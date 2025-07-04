package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFrameStream, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}

import scala.collection.mutable

class TaskTemplateKafkaLoadTopicByList(topicList: List[String], source: ConnectionTrait) extends TaskTemplate {
  override def run(parameters: mutable.Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    source match
      case x: QueryTrait => {
        return List.apply(DataFrameStream(x.getDataFrameBatchFromTopic(topicList.mkString(","))))
      }
      case _ => throw Exception()

    throw Exception()
  }
}
