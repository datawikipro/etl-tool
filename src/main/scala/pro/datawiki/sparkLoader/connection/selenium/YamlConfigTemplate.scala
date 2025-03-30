package pro.datawiki.sparkLoader.connection.selenium

import pro.datawiki.sparkLoader.YamlClass
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import java.time.Duration
import org.openqa.selenium.By
import org.openqa.selenium.WebDriver
import org.openqa.selenium.WebElement
import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.edge.EdgeDriver
import pro.datawiki.sparkLoader.configuration.parent.LogicClass

import scala.jdk.CollectionConverters.*

case class YamlConfigTemplate(
                               findElement: YamlConfigTemplateFindElement,
                               findElements: YamlConfigTemplateFindElements,
                               getDomAttribute: YamlConfigTemplateGetDomAttribute,
                               getDomProperty: YamlConfigTemplateGetDomProperty,
                               getText: YamlConfigTemplateGetText,
                             ) extends LogicClass {

  def getSubElements(webElement: WebElement): SeleniumList = {
    reset()
    setLogic(findElement)
    setLogic(findElements)
    setLogic(getDomAttribute)
    setLogic(getDomProperty)
    setLogic(getText)
    getLogic match
      case x: YamlConfigTemplateFindElement => return x.getSubElements(webElement)
      case x: YamlConfigTemplateFindElements => return x.getSubElements(webElement)
      case x: YamlConfigTemplateGetDomAttribute => return x.getSubElements(webElement)
      case x: YamlConfigTemplateGetDomProperty => return x.getSubElements(webElement)
      case x: YamlConfigTemplateGetText => return SeleniumList.applyByKeyValue(x.getSubElements(webElement))
      case _ => throw Exception()
    //  val box = webDriver.findElement(By.className("rt-Grid"))
    //  val list: List[WebElement] = box.findElements(By.tagName("a")).asScala.toList
    //  list.foreach(i => {
    //    println(i.getDomAttribute("href"))
    //    println(i.findElement(By.tagName("h4")).getText)
    //    println(i.findElement(By.tagName("p")).getText)
    //  })

  }
}
