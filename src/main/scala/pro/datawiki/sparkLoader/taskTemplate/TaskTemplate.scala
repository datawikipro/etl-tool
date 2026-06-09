package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.traits.LoggingTrait

trait TaskTemplate extends LoggingTrait {
  def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait]
}
