package pro.datawiki.diMigration

import pro.datawiki.diMigration.core.dictionary.OutputSystem
import pro.datawiki.sparkLoader.configuration.EltConfig.{getLines, mapper}
import pro.datawiki.yamlConfiguration.YamlClass

class AttributeYamlTarget(
                                config: String,
                                location: String,
                                templateLocation: String,
                                postfix: String ="",
                              ) {
  def getConfig:OutputSystem = {
    config match
      case "airflow" => return OutputSystem.Airflow
      case "etlTool" => return OutputSystem.EtlTool
      case "awsGlue" => return OutputSystem.AwsGlue
      case "x5" => return OutputSystem.X5
      case _ => throw Exception()
  }

  def getLocation:String = location
  def getPostfix:String = postfix
  def getTemplateLocation:String = templateLocation
}

object AttributeYamlTarget extends YamlClass {
  def apply(inLocation: String): AttributeYamlTarget = {
    val result:AttributeYamlTarget = mapper.readValue(getLines(inLocation), classOf[AttributeYamlTarget])
    return result
  }
}