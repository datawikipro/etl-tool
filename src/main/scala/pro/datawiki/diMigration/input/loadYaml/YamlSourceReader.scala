package pro.datawiki.diMigration.input.loadYaml

import pro.datawiki.diMigration.core.dag.CoreDag
import pro.datawiki.diMigration.core.dictionary.Schedule
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{YamlDag, YamlDataByTemplate}
import pro.datawiki.diMigration.input.traits.SourceTrait
import pro.datawiki.yamlConfiguration.YamlClass

class YamlSourceReader(
                        controlDagName: String,
                        controlDagSchedule: String,
                        dags: List[YamlDag] = List.apply(),
                        dataByTemplates: List[YamlDataByTemplate] = List.apply()
                      ) extends SourceTrait {
  override def getDagConfigs: List[CoreDag] = {
    val a = dags.flatMap(_.getCoreDag)
    val b = dataByTemplates.flatMap(_.getCoreDag)
    return a ++ b
  }
  
  override def getControlName: String = controlDagName

  override def getSchedule: Schedule = Schedule(controlDagSchedule)
}

object YamlSourceReader extends YamlClass {
  def apply(inLocation: String): YamlSourceReader = {
    val result: YamlSourceReader = mapper.readValue(getLines(inLocation), classOf[YamlSourceReader])
    return result
  }
}
