package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.context.TaskContext
import pro.datawiki.sparkLoader.task.*
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateAdHoc}

case class YamlConfigTransformationAdHoc(
                                          sourceObjectName: String,
                                          templateName: String,
                                          columnId: List[String] = List.apply(),
                                          asyncNumber: Int = throw Exception()
                                        ) extends YamlConfigTransformationTrait {

  override def getTaskTemplate: TaskTemplate = TaskTemplateAdHoc(sourceObjectName, templateName, columnId, asyncNumber)

  override def getTask(in: TaskTemplate): Task = {
    in match
      case x: TaskTemplateAdHoc => return TaskAdHoc(x, TaskContext.getTaskTemplate(templateName))
      case _ => throw UnsupportedOperationException("Unsupported TaskTemplate type for AdHoc transformation")
  }
}