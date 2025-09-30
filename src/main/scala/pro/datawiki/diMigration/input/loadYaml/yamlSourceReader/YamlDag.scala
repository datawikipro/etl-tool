package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader

import pro.datawiki.diMigration.core.dag.CoreDag
import pro.datawiki.diMigration.core.dictionary.Schedule
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDag.YamlTask
import pro.datawiki.exception.NotImplementedException

import java.util.Date

class YamlDag(
               dagName: String,
               startDate: Date,
               schedule: String,
               tasks: List[YamlTask] = List.apply(),
             ) {

  def getSchedule: Schedule = {
    schedule match
      case "hourly" => Schedule.Hourly
      case "daily" => Schedule.Daily
      case "all" => Schedule.All
      case "realtime" => Schedule.Realtime
  }

  def getCoreDag: List[CoreDag] = {
    throw NotImplementedException("getCoreDag method not implemented")
  }
}