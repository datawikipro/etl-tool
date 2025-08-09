package pro.datawiki.sparkLoader.task

import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.UnsupportedOperationException

import scala.collection.mutable

class TargetTemplateSimple(tableName: String) extends TargetTemplate {
  override def run(parameters: mutable.Map[String, String], isSync: Boolean): Unit = {
    throw new UnsupportedOperationException("TargetTemplateSimple.run not implemented")
  }
}
