package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement
import pro.datawiki.yamlConfiguration.LogicClass

import scala.jdk.CollectionConverters.*

trait YamlConfigTemplateFinderTrait {
  def getSelenium(webElement: WebElement): List[WebElement]

  def runTemplates(seqId: Int, in: WebElement): Map[String, SeleniumType]

  def mergeData(inMap: Map[String, SeleniumType], inAppend: Map[String, SeleniumType]): Map[String, SeleniumType]
}
