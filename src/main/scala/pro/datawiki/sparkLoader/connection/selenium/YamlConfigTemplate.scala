package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement
import pro.datawiki.yamlConfiguration.LogicClass

import scala.jdk.CollectionConverters.*
import java.time.LocalTime
import scala.collection.mutable

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
      case x: YamlConfigTemplateGetterTrait => {
        val result = x.getResult(webElement)
        return result
      }
      case _ => throw Exception()

  }

  def getModified(parameters: mutable.Map[String, String]): YamlConfigTemplate = {
    return YamlConfigTemplate(
      findElement     = if findElement     != null then findElement.getModified(parameters)     else null,
      findElements    = if findElements    != null then findElements.getModified(parameters)    else null,
      getDomAttribute = if getDomAttribute != null then getDomAttribute.getModified(parameters) else null,
      getDomProperty  = if getDomProperty  != null then getDomProperty.getModified(parameters)  else null,
      getText         = if getText         != null then getText.getModified(parameters)         else null,
    )

  }

}
