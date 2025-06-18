package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.task.{Task, TaskSimple, TaskTemplate, TaskTemplateExtractSchema}

case class YamlConfigTransformationExtractSchema(
                                                  tableName: String,
                                                  jsonColumn: String,
                                                  jsonResultColumn: String,
                                                  baseSchema: String
                                                ) extends YamlConfigTransformationTrait {
  override def getTaskTemplate: TaskTemplate = TaskTemplateExtractSchema(tableName, jsonColumn,jsonResultColumn,baseSchema)

  override def getTask(in: TaskTemplate): Task = TaskSimple(in)
}
