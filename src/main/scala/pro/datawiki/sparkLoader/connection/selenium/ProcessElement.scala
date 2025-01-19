package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement

object ProcessElement {
  
  private def processTemplate(webElement: WebElement,
                              template: YamlConfigTemplate
                      ): (List[KeyValue]) = {
    val keyValueResult: List[KeyValue] = template.getSubElements(webElement)
    return keyValueResult
  }

  def processTemplates(webElement: WebElement,
                       template: List[YamlConfigTemplate],
                       filter: YamlConfigTemplateFilter = YamlConfigTemplateFilter(elementId = null, varName = null, regexp = null),
                       action: String
                     ): List[KeyValue] = {
    var keyValueResult: List[KeyValue] = List.apply()
    template.foreach(i => {
      keyValueResult = keyValueResult ::: processTemplate(webElement = webElement, template = i)
    })

    if filter.isRegexp then {
      if !filter.checkRegexp(keyValueResult) then {
        return List.apply()
      }
    }

    action match
      case "click" => {
        webElement.click()
      }
      case null =>
      case _ => throw Exception()
    return keyValueResult
  }
}
