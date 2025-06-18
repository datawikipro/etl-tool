package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap

import pro.datawiki.sparkLoader.task.TaskTemplateIdMapConfig

class YamlConfigTransformationIdMapMerge(
                                               alias: String,
                                               restoreRk:Boolean = false,
                                               in: YamlConfigTransformationIdMapBaseConfig,
                                               out: YamlConfigTransformationIdMapBaseConfig
                                             ) {
  def getTaskTemplateIdMapConfig: TaskTemplateIdMapConfig = in.getTaskTemplateIdMapConfig
  def getAlias: String ={
    if alias == null then
      throw Exception()
    return alias
  }
  def getRestoreRk: Boolean = restoreRk
  def getIn:YamlConfigTransformationIdMapBaseConfig = in

  def getOut: YamlConfigTransformationIdMapBaseConfig = out

}
