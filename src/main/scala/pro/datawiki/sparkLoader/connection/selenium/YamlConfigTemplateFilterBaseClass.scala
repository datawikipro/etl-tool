package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement

import scala.collection.mutable

class YamlConfigTemplateFilterBaseClass(
                                         filterById: Int,
                                         filterByRegexp: YamlConfigTemplateFilterByRegexp
                                       ) {
  def isFilteredBeforeParse(webElement: WebElement, index: Int): Boolean = {
    if !(filterById == -2147483648) then
      if !(index == filterById) then return false
    return true
  }

  def isFilteredAfterParse(keyValueResult: Map[String, SeleniumType]): Boolean = {
    if !(filterByRegexp == null) then
      if !filterByRegexp.checkRegexp(keyValueResult) then
        return false
    return true
  }

}
