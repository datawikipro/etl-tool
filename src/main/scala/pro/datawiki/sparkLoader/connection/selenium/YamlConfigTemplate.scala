package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement
import pro.datawiki.yamlConfiguration.LogicClass

import scala.jdk.CollectionConverters.*
import java.time.LocalTime


case class YamlConfigTemplate(
                               findElement: YamlConfigTemplateFindElement,
                               findElements: YamlConfigTemplateFindElements,
                               getDomAttribute: YamlConfigTemplateGetDomAttribute,
                               getDomProperty: YamlConfigTemplateGetDomProperty,
                               getText: YamlConfigTemplateGetText,
                             ) extends LogicClass {

  def getSubElements(webElement: WebElement): Map[String, SeleniumType] = {
    val logic = getLogic(findElement, findElements, getDomAttribute, getDomProperty, getText)
    logic match
      case x: YamlConfigTemplateFinderTrait => {
        val list: List[WebElement] = x.getSelenium(webElement)
        var newMap: Map[String, SeleniumType] = Map.apply()
        list.zipWithIndex.foreach((p, index) => {
          val tmpMap= x.runTemplates(index,p)
          newMap = x.mergeData(newMap,tmpMap)
        })
        return newMap
      }
      case x: YamlConfigTemplateGetterTrait =>
        val result = x.getResult(webElement)
        return result
      case _ => throw Exception()

  }

}
