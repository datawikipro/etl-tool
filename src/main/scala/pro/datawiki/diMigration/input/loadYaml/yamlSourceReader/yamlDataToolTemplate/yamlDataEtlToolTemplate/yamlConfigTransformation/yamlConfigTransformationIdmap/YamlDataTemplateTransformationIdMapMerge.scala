package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation.yamlConfigTransformationIdmap

import pro.datawiki.sparkLoader.taskTemplate.TaskTemplateIdMapConfig

class YamlDataTemplateTransformationIdMapMerge(
                                                alias: String,
                                                restoreRk: Boolean = false,
                                                in: YamlDataTemplateTransformationIdMapBaseConfig,
                                                out: YamlDataTemplateTransformationIdMapBaseConfig
                                              )