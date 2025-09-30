package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap

import pro.datawiki.sparkLoader.taskTemplate.TaskTemplateIdMapConfig

case class YamlConfigTransformationIdMapConfig(
                                                systemCode: String,
                                                columnNames: List[String],
                                                domainName: String,
                                                alias: String
                                              ) {
  def getTaskTemplateIdMapConfig: TaskTemplateIdMapConfig = {
    TaskTemplateIdMapConfig(systemCode, columnNames, domainName)
  }

  def getAlias: String = {
    if alias == null then return domainName
    return alias
  }
}