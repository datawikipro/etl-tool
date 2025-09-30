package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.By
import pro.datawiki.yamlConfiguration.LogicClass

import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateBy(className: String, tagName: String, ById: String, ByXpath: String, byCssSelector: String) extends LogicClass {

  def getBy: By = {
    val value = getLogic(className, tagName, ById, ByXpath, byCssSelector)

    if className != null then return By.className(className)
    if tagName != null then return By.tagName(tagName)
    if ById != null then return By.ById(ById)
    if ByXpath != null then return By.xpath(ByXpath)
    if byCssSelector != null then return By.cssSelector(s"""[${byCssSelector}]""")
    throw IllegalArgumentException("At least one By selector must be specified")
  }

}
