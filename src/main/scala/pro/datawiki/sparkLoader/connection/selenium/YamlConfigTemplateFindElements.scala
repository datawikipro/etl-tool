package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement
import pro.datawiki.sparkLoader.LogMode

import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateFindElements(
                                           byClassName: String,
                                           byTagName: String,
                                           byId: String,
                                           byCssSelector: String,
                                           byXpath: String,
                                           filterById: Int = -2147483648,
                                           filterByRegexp: YamlConfigTemplateFilterByRegexp,
                                           template: List[YamlConfigTemplate],
                                           splitResult: String,
                                           action: String,
                                           isMayBeEmpty: Boolean = false
                                         ) extends YamlConfigTemplateFilterBaseClass(filterById, filterByRegexp),
  YamlConfigTemplateFinderTrait {
  val by: YamlConfigTemplateBy = YamlConfigTemplateBy(className = byClassName, tagName = byTagName, ById = byId, ByXpath = byXpath, byCssSelector = byCssSelector)

  private def isSplitResult: Boolean = {
    splitResult match
      case null => return false
      case _ => return true
  }


  override def getSelenium(webElement: WebElement): List[WebElement] = {
    val elems = webElement.findElements(by.getBy).asScala.toList
    if elems.isEmpty then
      if isMayBeEmpty then return List.apply()
    //throw Exception()//TODO
    return elems
  }

  override def runTemplates(id: Int, webElement: WebElement): Map[String, SeleniumType] = {
    try {
      var newMap: Map[String, SeleniumType] = Map.apply()
      var list: List[Map[String, SeleniumType]] = List.apply()

      if !isFilteredBeforeParse(webElement, id) then return Map()

      LogMode.debugSelenium(webElement)

      template.foreach(i => {
        newMap ++= i.getSubElements(webElement = webElement)
      })

      if !isFilteredAfterParse(newMap) then
        return Map()

      action match
        case "click" => {
          webElement.click()
        }
        case null =>
        case _ =>
          throw Exception()
      return newMap
    } catch
      case _ => return Map()
  }

  def mergeData(inMap: Map[String, SeleniumType], inAppend: Map[String, SeleniumType]): Map[String, SeleniumType] = {
    if !isSplitResult then {
      return inMap ++ inAppend
    }

    val a: SeleniumType = inMap.getOrElse(splitResult, SeleniumArray(List.empty[Map[String, SeleniumType]]))

    val b: List[Map[String, SeleniumType]] = a match
      case x: SeleniumArray => x.getList
      case _ => throw Exception()

    val d: Map[String, SeleniumType] = inMap + (splitResult -> SeleniumArray(b.appended(inAppend)))
    return d

  }

}