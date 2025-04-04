package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.{By, WebDriver, WebElement}
import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.edge.EdgeDriver
import pro.datawiki.sparkLoader.{LogMode, YamlClass}
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import java.time.Duration
import scala.jdk.CollectionConverters.*

class YamlConfigTemplateFindElement(
                                     by: YamlConfigTemplateBy,
                                     template: List[YamlConfigTemplate] = List.apply(),
                                     ignoreError: Boolean,
                                     action: String
                                   ) {

  def getSubElements(webElement: WebElement): SeleniumList = {
    try {
      val elem = webElement.findElement(by.getBy)
      if LogMode.isDebug then {
        println(elem.getDomProperty("innerHTML"))
      }
      val keyValueResult = ProcessElement.processTemplates(webElement = elem, template = template, action = action)
      return keyValueResult
    }
//    catch
//      case _ => {
//        if ignoreError then return SeleniumList.apply()
//        throw Exception()
//      }
  }
}
