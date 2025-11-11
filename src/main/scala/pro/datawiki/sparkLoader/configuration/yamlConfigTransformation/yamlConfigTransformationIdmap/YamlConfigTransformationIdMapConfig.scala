package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap

import pro.datawiki.sparkLoader.taskTemplate.TaskTemplateIdMapConfig

case class YamlConfigTransformationIdMapConfig(
                                                systemCode: String,
                                                columnNames: List[String],
                                                domainName: String,
                                                timeColumn: String,
                                                secondForExpire: Int = -1,
                                                alias: String
                                              ) {
  def getTaskTemplateIdMapConfig: TaskTemplateIdMapConfig = {
    TaskTemplateIdMapConfig(systemCode, columnNames, domainName,timeColumn,secondForExpire)
  }

  def getAlias: String = {
    if alias == null then return domainName
    return alias
  }
}