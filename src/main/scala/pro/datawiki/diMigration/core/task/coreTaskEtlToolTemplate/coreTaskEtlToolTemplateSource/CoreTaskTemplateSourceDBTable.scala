package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource

import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn

case class CoreTaskTemplateSourceDBTable(
                                          tableSchema: String,
                                          tableName: String,
                                          tableColumns: List[YamlConfigSourceDBTableColumn] = List.apply(),
                                          filter: String = null,
                                          limit: Int = 0)