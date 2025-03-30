package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement
import pro.datawiki.sparkLoader.configuration.parent.LogicClass

import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateGetDomProperty(
                                              value: String,
                                              parameter: YamlConfigTemplateParameter
                                            ) extends LogicClass {

  def getSubElements(webElement: WebElement): SeleniumList = {
    val result: String = webElement.getDomProperty(value)
    return parameter.getParametersResult(result)
  }
} 