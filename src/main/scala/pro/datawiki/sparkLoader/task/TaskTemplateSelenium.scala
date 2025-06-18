package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, QueryTrait}

import scala.collection.mutable

class TaskTemplateSelenium(source: LoaderSelenium) extends TaskTemplate {
  override def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait] = {
    List.apply(source.run(parameters, isSync))
  }
}
