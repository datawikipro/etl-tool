package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.{By, WebDriver, WebElement}
import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.edge.EdgeDriver
import pro.datawiki.sparkLoader.YamlClass
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import java.time.Duration
import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateBy(
                               function: String,
                               value:String
                     ) {
  def getBy:By= {
    function match
      case "className" => return By.className(value)
      case "tagName" => return By.tagName(value)
      case "ById" => return By.ById(value)
      case "ByXpath" => return By.xpath(value)
      case _ => throw Exception()
  }
  
}
