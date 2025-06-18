package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import org.apache.spark.sql.Row
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.task.*

class YamlConfigTransformationAdHoc(
                                     sourceObjectName: String,
                                     templateName: String,
                                     columnId: List[String] = List.apply(),
                                     asyncNumber: Int = 8
                                   ) extends YamlConfigTransformationTrait {

  override def getTaskTemplate: TaskTemplate = TaskTemplateAdHoc(sourceObjectName, templateName, columnId, asyncNumber)

  override def getTask(in: TaskTemplate): Task = {
    in match
      case x: TaskTemplateAdHoc => return TaskAdHoc(x, Context.getTaskTemplate(templateName))
      case _ => throw Exception()
  }
}