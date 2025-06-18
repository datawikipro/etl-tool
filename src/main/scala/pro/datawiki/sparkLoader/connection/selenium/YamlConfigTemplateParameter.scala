package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement
import pro.datawiki.yamlConfiguration.LogicClass

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateParameter(
                                        simple: String,
                                        regexpLogic: YamlConfigTemplateParameterRegexp
                                      ) extends LogicClass {
  
  def getResult(value: String): Map[String, SeleniumType] = {
    super.getLogic(simple, regexpLogic) match
      case x: String => Map(x ->SeleniumString(value))
      case x: YamlConfigTemplateParameterRegexp => x.getResult(value)
  }
}
