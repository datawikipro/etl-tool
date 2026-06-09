package pro.datawiki.sparkLoader.task

import pro.datawiki.exception.UnsupportedOperationException

class TargetTemplateSimple(tableName: String) extends TargetTemplate {
  override def run(parameters: Map[String, String], isSync: Boolean): Unit = {
    throw UnsupportedOperationException("TargetTemplateSimple.run not implemented")
  }
}
