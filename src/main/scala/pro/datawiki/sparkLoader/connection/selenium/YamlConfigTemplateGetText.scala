package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement
import pro.datawiki.yamlConfiguration.LogicClass

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateGetText(
                                      parameter: String
                                    ) extends LogicClass, YamlConfigTemplateGetterTrait {
  def getResult(webElement: WebElement): Map[String, SeleniumType] = {
    var result: Map[String, SeleniumType] = Map(parameter -> SeleniumString(webElement.getText))
    return result
  }

  def getModified(parameters: mutable.Map[String, String]): YamlConfigTemplateGetText = {
    return YamlConfigTemplateGetText(
      parameter = YamlConfig.getModifiedString(parameter, parameters)
    )

  }
}
