package pro.datawiki.diMigration

import pro.datawiki.diMigration.core.dictionary.OutputSystem
import pro.datawiki.sparkLoader.configuration.EltConfig.{getLines, mapper}
import pro.datawiki.yamlConfiguration.YamlClass

case class AttributeYaml(
                          transformations: List[AttributeYamlTransform]
                        )

object AttributeYaml extends YamlClass {
  def apply(inLocation: String): AttributeYaml = {
    val result: AttributeYaml = mapper.readValue(getLines(inLocation), classOf[AttributeYaml])
    return result
  }
}