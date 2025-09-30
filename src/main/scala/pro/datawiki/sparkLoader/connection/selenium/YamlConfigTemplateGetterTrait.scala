package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement
import pro.datawiki.yamlConfiguration.LogicClass

import scala.jdk.CollectionConverters.*

trait YamlConfigTemplateGetterTrait {
  def getResult(webElement: WebElement): Map[String, SeleniumType]
}
