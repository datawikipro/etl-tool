package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.edge.EdgeDriver
import org.openqa.selenium.{By, WebDriver, WebElement}
import pro.datawiki.sparkLoader.YamlClass
import pro.datawiki.sparkLoader.configuration.parent.LogicClass
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import java.time.Duration
import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateParameter(
                                        simple: String,
                                        regexpLogic: YamlConfigTemplateParameterRegexp
                     ) extends LogicClass {
  def getParametersResult(value:String):List[KeyValue] = {
    reset()
    setLogic(simple)
    setLogic(regexpLogic)
    getLogic match
      case x: String => return List.apply(KeyValue(x,value))
      case x: YamlConfigTemplateParameterRegexp => return x.getResultParse(value)
  }
  
}
