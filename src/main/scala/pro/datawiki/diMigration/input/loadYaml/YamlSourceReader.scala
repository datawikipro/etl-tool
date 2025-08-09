package pro.datawiki.diMigration.input.loadYaml

import pro.datawiki.diMigration.core.dag.CoreDag
import pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.{YamlDag, YamlDataByTemplate}
import pro.datawiki.diMigration.input.traits.SourceTrait
import pro.datawiki.yamlConfiguration.YamlClass

class YamlSourceReader(dags: List[YamlDag] = List.apply(),
                       dataByTemplates: List[YamlDataByTemplate] = List.apply()
                      ) extends SourceTrait {
  override def getDagConfigs: List[CoreDag] = {
    val configs = dags ++ dataByTemplates
    if configs.isEmpty then throw Exception("Config list is empty")

    return configs.flatMap(_.getCoreDag)


  }

}

object YamlSourceReader extends YamlClass {
  def apply(inLocation: String): YamlSourceReader = {
    val result: YamlSourceReader = mapper.readValue(getLines(inLocation), classOf[YamlSourceReader])
    return result
  }
}
