package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.DataFrameTrait

import scala.collection.mutable

trait TaskTemplate {
  def run(parameters: mutable.Map[String, String], isSync:Boolean): List[DataFrameTrait]
}
