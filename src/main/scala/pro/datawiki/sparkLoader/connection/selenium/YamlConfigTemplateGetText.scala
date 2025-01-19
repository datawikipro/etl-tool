package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.{By, WebDriver, WebElement}
import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.edge.EdgeDriver
import pro.datawiki.sparkLoader.YamlClass
import pro.datawiki.sparkLoader.configuration.parent.LogicClass
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import java.time.Duration
import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateGetText(
                                      parameter:String
                     ) extends LogicClass {
  
  def getSubElements(webElement: WebElement): KeyValue = {
     return KeyValue(parameter, webElement.getText)
  }
}
