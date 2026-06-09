package pro.datawiki.yamlConfiguration

import pro.datawiki.sparkLoader.context.ApplicationContext

extension (yaml: YamlClass) {
  def getLinesGlobalContext(inConfig: String): String = {
    YamlClass.getLines(inConfig, ApplicationContext.getGlobalVariables)
  }
}