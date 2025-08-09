package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTarget

import pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateTarget.CoreTaskTemplateTargetColumn

case class YamlDataTemplateTargetColumn(columnName: String,
                                        isNewCCD: Boolean,
                                        domainName: String,
                                        tenantName: String,
                                        isNullable: Boolean
                                       ) {
  def getCoreTaskTemplateTargetColumn: CoreTaskTemplateTargetColumn = CoreTaskTemplateTargetColumn(columnName, isNewCCD, domainName, tenantName, isNullable)
}