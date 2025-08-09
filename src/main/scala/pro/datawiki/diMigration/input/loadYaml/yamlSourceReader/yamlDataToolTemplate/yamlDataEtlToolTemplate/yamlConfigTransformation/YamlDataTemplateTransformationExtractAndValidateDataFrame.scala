package pro.datawiki.diMigration.input.loadYaml.yamlSourceReader.yamlDataToolTemplate.yamlDataEtlToolTemplate.yamlConfigTransformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.task.{Task, TaskSimple, TaskTemplate, TaskTemplateExtractSchema}

case class YamlDataTemplateTransformationExtractAndValidateDataFrame(
                                                                      dataFrameIn: String,
                                                                     configLocation: String
                                                ) 