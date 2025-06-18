package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement
import pro.datawiki.yamlConfiguration.LogicClass

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateGetDomAttribute(
                                              value: String,
                                              parameter: YamlConfigTemplateParameter
                                            ) extends LogicClass,YamlConfigTemplateGetterTrait {

  def getResult(webElement: WebElement): Map[String, SeleniumType] = {
    val result: String = webElement.getDomAttribute(value)
    return parameter.getResult(result)
  }

} 