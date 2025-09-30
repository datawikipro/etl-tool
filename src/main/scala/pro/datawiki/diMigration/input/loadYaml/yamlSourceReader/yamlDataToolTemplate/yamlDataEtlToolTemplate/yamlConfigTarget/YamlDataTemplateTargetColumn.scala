package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.coreTaskTemplateTargetDatabase.CoreTaskTemplateTargetColumn

case class YamlDataTemplateTargetColumn(columnName: String,
                                        isNullable: Boolean,
                                        columnType: String,
                                        columnTypeDecode: Boolean
                                       )