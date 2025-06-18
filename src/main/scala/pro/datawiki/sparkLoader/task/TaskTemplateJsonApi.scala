package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.connection.jsonApi.LoaderJsonApi

import scala.collection.mutable

class TaskTemplateJsonApi(source: LoaderJsonApi) extends TaskTemplate {
  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    List.apply(source.run(parameters))
  }
}
