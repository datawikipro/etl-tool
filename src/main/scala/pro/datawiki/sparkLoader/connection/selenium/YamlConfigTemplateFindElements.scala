package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.LogMode
import pro.datawiki.sparkLoader.context.SparkContext
import pro.datawiki.sparkLoader.task.Task
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplateSparkSql

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

case class YamlConfigTemplateFindElements(
                                           byClassName: String,
                                           byTagName: String,
                                           byId: String,
                                           byCssSelector: String,
                                           byXpath: String,
                                           filterById: Int = -2147483648,
                                           filterByRegexp: YamlConfigTemplateFilterByRegexp,
                                           template: List[YamlConfigTemplate] = List.apply(),
                                           splitResult: String,
                                           action: String,
                                           sleep: Int,
                                           setValueString: String,
                                           setValueFromBroker: String,
                                           isMayBeEmpty: Boolean = false
                                         ) extends YamlConfigTemplateFilterBaseClass(filterById, filterByRegexp), YamlConfigTemplateFinderTrait {

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
    // TODO: добавить реализацию
    return elems
  }

  override def runTemplates(id: Int, webElement: WebElement): Map[String, SeleniumType] = {
    var newMap: Map[String, SeleniumType] = Map.apply()
    var list: List[Map[String, SeleniumType]] = List.apply()

    if !isFilteredBeforeParse(webElement, id) then return Map()

    LogMode.debugSelenium(webElement)

    template.foreach(i => {
      newMap ++= i.getSubElements(webElement = webElement)
    })

    if !isFilteredAfterParse(newMap) then {
      return Map()
    }

    SeleniumAction(action) match {
      case SeleniumAction.click => {
        webElement.click()
      }
      case SeleniumAction.none => {

      }
      case _ => {
        throw UnsupportedOperationException("Unsupported Selenium action")
      }
    }
    if !(setValueString == null) then {
      webElement.sendKeys(setValueString)
    }
    if !(setValueFromBroker == null) then {
      while (!SparkContext.isDefinedTable(setValueFromBroker)) {
        Thread.sleep(1000)
        println("test")
      }

      val x = TaskTemplateSparkSql(sql = s"select * from ${setValueFromBroker}")
      val t: Map[String, String] = Map()
      val dataFromConsumer: List[DataFrameTrait] = x.run(t, true)
      dataFromConsumer.length match {
        case 1 => {
          val dataFrom = dataFromConsumer.head.getDataFrame.collect()
          val value = dataFrom.head.get(0)
          val value2 = value match {
            case x: String => x
            case x: Int => s"$x"
            case _ => throw UnsupportedOperationException("Unsupported value type for Selenium input")
          }
          webElement.sendKeys(value2)
        }
        case _ => throw UnsupportedOperationException("Unsupported number of data elements")
      }
    }
    Thread.sleep(sleep * 1000)
    return newMap
  }

  def mergeData(inMap: Map[String, SeleniumType], inAppend: Map[String, SeleniumType]): Map[String, SeleniumType] = {
    if !isSplitResult then {
      return inMap ++ inAppend
    }

    val a: SeleniumType = inMap.getOrElse(splitResult, SeleniumArray(List.empty[Map[String, SeleniumType]]))

    val b: List[Map[String, SeleniumType]] = a match
      case x: SeleniumArray => x.getList
      case _ => throw UnsupportedOperationException("Unsupported Selenium type")

    val d: Map[String, SeleniumType] = inMap + (splitResult -> SeleniumArray(b.appended(inAppend)))
    return d

  }

  def getModified(parameters: Map[String, String]): YamlConfigTemplateFindElements = {
    var newTemplate: List[YamlConfigTemplate] = List.apply()
    template.foreach(i => {
      newTemplate = newTemplate.appended(i.getModified(parameters))
    })

    return YamlConfigTemplateFindElements(
      byClassName = YamlConfig.getModifiedString(byClassName, parameters),
      byTagName = YamlConfig.getModifiedString(byTagName, parameters),
      byId = YamlConfig.getModifiedString(byId, parameters),
      byXpath = YamlConfig.getModifiedString(byXpath, parameters),
      byCssSelector = YamlConfig.getModifiedString(byCssSelector, parameters),
      filterById = filterById,
      filterByRegexp = if filterByRegexp != null then filterByRegexp.getModified(parameters) else null,
      template = newTemplate,
      splitResult = YamlConfig.getModifiedString(splitResult, parameters),
      setValueString = YamlConfig.getModifiedString(setValueString, parameters),
      setValueFromBroker = YamlConfig.getModifiedString(setValueFromBroker, parameters),
      sleep = sleep,
      isMayBeEmpty = isMayBeEmpty,
      action = YamlConfig.getModifiedString(action, parameters),
    )
  }

}