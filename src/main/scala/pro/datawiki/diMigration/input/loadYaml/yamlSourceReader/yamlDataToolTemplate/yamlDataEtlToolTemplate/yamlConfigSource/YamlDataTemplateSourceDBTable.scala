package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigSource

import com.fasterxml.jackson.annotation.JsonInclude
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.{YamlConfigSourceDBTableColumn, YamlConfigSourceDBTablePartition}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlDataTemplateSourceDBTable(
                                    tableSchema: String,
                                    tableName: String,
                                    tableColumns: List[YamlConfigSourceDBTableColumn] = List.apply(),
                                    partitionBy: List[YamlConfigSourceDBTablePartition] = List.apply(),
                                    filter: String=null,
                                    limit: Int=0)