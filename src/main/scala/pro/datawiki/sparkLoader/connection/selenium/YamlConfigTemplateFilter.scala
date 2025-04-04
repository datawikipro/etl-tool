package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.edge.EdgeDriver
import org.openqa.selenium.{By, WebDriver, WebElement}
import pro.datawiki.sparkLoader.YamlClass
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import java.time.Duration
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

class YamlConfigTemplateFilter(
                                elementId: String,
                                varName: String,
                                regexp: String
                              ) {
  def isElementId:Boolean = {
    if elementId == null then return false
    return true
  }

  def isRegexp: Boolean = {
    if varName == null && regexp == null then return false
    if varName == null || regexp == null then throw Exception()

    return true
  }

  def checkRegexp(in:SeleniumList): Boolean = {
    in.getList.foreach(i=>
      if i.key == varName then {
        val pattern1: Regex = s"${regexp}".r
        val res = i.value match
          case x: SeleniumString => x.getValue
          case _=> throw Exception()
        val tst = pattern1.matches(res)
        return tst
      }
    )
    return false
  }


  def getElementId: Int = {
    return elementId.toInt
  }
}
