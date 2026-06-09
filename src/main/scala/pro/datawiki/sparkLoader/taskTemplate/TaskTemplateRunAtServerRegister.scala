package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.DataFrameTrait

trait TaskTemplateRunAtServerRegister {
  def getAtServerCode(targetTable:String,parameters: Map[String, String], isSync: Boolean): Boolean
}
