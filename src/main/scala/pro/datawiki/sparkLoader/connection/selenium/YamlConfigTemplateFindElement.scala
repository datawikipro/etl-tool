package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.{By, WebDriver, WebElement}
import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.edge.EdgeDriver
import pro.datawiki.sparkLoader.YamlClass
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import java.time.Duration
import scala.jdk.CollectionConverters.*

class YamlConfigTemplateFindElement(
                                     by: YamlConfigTemplateBy,
                                     template: List[YamlConfigTemplate] = List.apply(),
                                     ignoreError: Boolean,
                                     action: String
                                   ) {

  def getSubElements(webElement: WebElement): (List[KeyValue]) = {
    try {
      val elem = webElement.findElement(by.getBy)
      val keyValueResult = ProcessElement.processTemplates(webElement = elem, template = template, action = action)
      return keyValueResult
    }
    catch
      case _ => {
        if ignoreError then return List.apply()
        throw Exception()
      }
  }
}
