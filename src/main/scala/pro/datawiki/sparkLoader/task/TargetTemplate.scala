package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.DataFrameTrait

import scala.collection.mutable

trait TargetTemplate {
  def run(parameters: mutable.Map[String, String], isSync:Boolean): Unit
}
