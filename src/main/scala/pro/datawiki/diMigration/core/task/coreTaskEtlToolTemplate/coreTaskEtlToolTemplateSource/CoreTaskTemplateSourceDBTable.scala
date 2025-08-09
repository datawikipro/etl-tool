package pro.datawiki.diMigration.core.task.coreTaskEtlToolTemplate.coreTaskEtlToolTemplateSource

import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.{YamlConfigSourceDBTableColumn, YamlConfigSourceDBTablePartition}

case class CoreTaskTemplateSourceDBTable(
                                    tableSchema: String,
                                    tableName: String,
                                    tableColumns: List[YamlConfigSourceDBTableColumn] = List.apply(),
                                    partitionBy: List[YamlConfigSourceDBTablePartition] = List.apply(),
                                    filter: String=null,
                                    limit: Int=0)