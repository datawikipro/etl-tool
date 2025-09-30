package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement
import pro.datawiki.yamlConfiguration.LogicClass

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateGetDomProperty(
                                             value: String,
                                             parameter: YamlConfigTemplateParameter
                                           ) extends LogicClass, YamlConfigTemplateGetterTrait {
  override def getResult(webElement: WebElement): Map[String, SeleniumType] = {
    val result: String = webElement.getDomProperty(value)
    return parameter.getResult(result)
  }

  def getModified(parameters:  Map[String, String]): YamlConfigTemplateGetDomProperty = {
    return new YamlConfigTemplateGetDomProperty(
      value = YamlConfig.getModifiedString(value, parameters),
      parameter = parameter.getModified(parameters)
    )

  }

}