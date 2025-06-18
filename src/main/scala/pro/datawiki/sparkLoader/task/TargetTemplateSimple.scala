package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.DataFrameTrait

import scala.collection.mutable

class TargetTemplateSimple(tableName: String) extends TargetTemplate {
  override def run(parameters: mutable.Map[String, String], isSync: Boolean): Unit = {
    throw Exception()
  }
}
