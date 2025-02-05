package pro.datawiki.sparkLoader.connection.selenium

import org.openqa.selenium.WebElement

object ProcessElement {
  
  private def processTemplate(webElement: WebElement,
                              template: YamlConfigTemplate
                      ): SeleniumList = {
    val keyValueResult: SeleniumList = template.getSubElements(webElement)
    return keyValueResult
  }

  def processTemplates(webElement: WebElement,
                       template: List[YamlConfigTemplate],
                       filter: YamlConfigTemplateFilter = YamlConfigTemplateFilter(elementId = null, varName = null, regexp = null),
                       action: String
                     ): SeleniumList = {
    var keyValueResult: SeleniumList = SeleniumList.apply()
    if filter.isRegexp then {
println("a")
    }
    template.foreach(i => {
      keyValueResult.appendElements(processTemplate(webElement = webElement, template = i))
    })

    if filter.isRegexp then {
      if !filter.checkRegexp(keyValueResult) then {
        return SeleniumList.apply()
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
