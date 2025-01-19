package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.edge.EdgeDriver
import org.openqa.selenium.{By, WebDriver, WebElement}
import pro.datawiki.sparkLoader.YamlClass
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import java.time.Duration
import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateFindElements(
                                           by: YamlConfigTemplateBy,
                                           filter: YamlConfigTemplateFilter = YamlConfigTemplateFilter(elementId=null,      varName=null,      regexp=null),
                                           template: List[YamlConfigTemplate],
                                           splitResult: String,
                                           action: String
                                         ) {


  def isSplitResult: Boolean = {
    splitResult match
      case null => return false
      case _ => return true
  }

  def getSubElements(webElement: WebElement): (List[KeyValue]) = {
    var elems = webElement.findElements(by.getBy).asScala.toList
    //TODO
    if filter.isElementId then {
       elems = List.apply(elems(filter.getElementId))
    }
    var seleniumSplitResult: List[List[KeyValue]] = List.apply()
    var seleniumResult: List[KeyValue] = List.apply()

    elems.foreach(elem => {
      val keyValueResult: List[KeyValue] = ProcessElement.processTemplates(webElement = elem, template = template,filter = filter, action=action)
      isSplitResult match
        case true => seleniumSplitResult = seleniumSplitResult :+ keyValueResult
        case false => seleniumResult = seleniumResult ::: keyValueResult
    })
    if isSplitResult then {
      return List.apply(KeyValue(splitResult,seleniumSplitResult))
    }
    return (seleniumResult)

  }
}