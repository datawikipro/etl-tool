package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement
import pro.datawiki.sparkLoader.LogMode

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateFindElement(
                                          byClassName: String,
                                          byTagName: String,
                                          byId: String,
                                          byXpath: String,
                                          byCssSelector: String,
                                          template: List[YamlConfigTemplate] = List.apply(),
                                          isMayBeEmpty: Boolean
                                        ) extends YamlConfigTemplateFinderTrait {
  val by: YamlConfigTemplateBy = YamlConfigTemplateBy(className = byClassName, tagName = byTagName, ById = byId, ByXpath = byXpath, byCssSelector = byCssSelector)

  def getSelenium(webElement: WebElement): List[WebElement] = {
    val elem = webElement.findElements(by.getBy)
    elem.size() match
      case 0 => {
        if isMayBeEmpty then return List.apply()
        throw IllegalArgumentException("Element not found and cannot be empty")
      }
      case 1 => {
        LogMode.debugSelenium(elem.get(0))
        return List.apply(elem.get(0))
      }
      case _ => {
        return elem.asScala.toList //TODO
      }
  }

  def runTemplates(seqId: Int, in: WebElement): Map[String, SeleniumType] = {
    var keyValueResult: Map[String, SeleniumType] = Map()

    template.foreach(i => {
      val results = i.getSubElements(webElement = in)
      keyValueResult ++= results
    })


    return keyValueResult
  }

  override def mergeData(inMap: Map[String, SeleniumType], inAppend: Map[String, SeleniumType]): Map[String, SeleniumType] = {
    return inMap ++ inAppend
  }


  def getModified(parameters: Map[String, String]): YamlConfigTemplateFindElement = {
    return YamlConfigTemplateFindElement(
      byClassName = YamlConfig.getModifiedString(byClassName, parameters),
      byTagName = YamlConfig.getModifiedString(byTagName, parameters),
      byId = YamlConfig.getModifiedString(byId, parameters),
      byXpath = YamlConfig.getModifiedString(byXpath, parameters),
      byCssSelector = YamlConfig.getModifiedString(byCssSelector, parameters),
      template = {
        var newTemplate: List[YamlConfigTemplate] = List.apply()
        template.foreach(i => {
          newTemplate = newTemplate.appended(i.getModified(parameters))
        })
        newTemplate
      },
      isMayBeEmpty = isMayBeEmpty,
    )

  }
}
