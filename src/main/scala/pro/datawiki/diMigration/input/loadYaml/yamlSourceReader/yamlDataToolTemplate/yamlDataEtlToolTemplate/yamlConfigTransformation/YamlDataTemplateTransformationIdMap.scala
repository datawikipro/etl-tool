package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation

import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap.{YamlConfigTransformationIdMapConfig, YamlConfigTransformationIdMapMerge}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.task.*

import scala.collection.mutable

case class YamlDataTemplateTransformationIdMap(
                                                sourceName: String,
                                                connection: String,
                                                idMapGenerate: List[YamlConfigTransformationIdMapConfig] = List.apply(),
                                                idMapRestore: List[YamlConfigTransformationIdMapConfig] = List.apply(),
                                                idMapMerge: List[YamlConfigTransformationIdMapMerge] = List.apply()
                                              )