package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.edge.EdgeDriver
import org.openqa.selenium.{By, WebDriver, WebElement}
import pro.datawiki.sparkLoader.YamlClass
import pro.datawiki.sparkLoader.configuration.parent.LogicClass
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import java.time.Duration
import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateGetDomAttribute(
                                              value: String,
                                              parameter: YamlConfigTemplateParameter
                                            ) extends LogicClass {

  def getSubElements(webElement: WebElement): SeleniumList = {
    val result: String = webElement.getDomAttribute(value)
    return parameter.getParametersResult(result)
  }
} 