package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader

import pro.datawiki.diMigration.core.dag.CoreDag
import pro.datawiki.diMigration.core.dictionary.Schedule
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDag.YamlTask

import java.util.Date

class YamlDag(
               dagName: String,
               startDate: Date,
               schedule: String,
               tasks: List[YamlTask] = List.apply(),
             ) extends YamlDataToolTemplate {
  
   def getSchedule: Schedule = {
    schedule match
      case "hourly" => Schedule.Hourly
      case "daily" => Schedule.Daily
      case "all" => Schedule.All
  }

  override def getCoreDag: List[CoreDag] = {
    throw Exception()
  }
}