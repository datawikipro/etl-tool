package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.*
import pro.datawiki.sparkLoader.task.{Task, TaskTemplate}
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.yamlConfiguration.LogicClass

class YamlConfigTransformation(objectName: String,
                               cache: String,
                               idMap: YamlConfigTransformationIdMap,
                               sparkSql: YamlConfigTransformationSparkSql,
                               extractSchema: YamlConfigTransformationExtractSchema,
                               adHoc: YamlConfigTransformationAdHoc,

                              ) extends LogicClass {
  def getObjectName: String = objectName

  def getLogic: Any = {
    super.getLogic(idMap, sparkSql, extractSchema, adHoc)
  }

  def getTransformation: YamlConfigTransformationTrait = {
    getLogic match
      case x: YamlConfigTransformationTrait => return x
      case _ => throw Exception()
  }
  
  def createTask(): Task = {
    val task: Task = getLogic match
      case x: YamlConfigTransformationTrait => {
        val taskTemplate: TaskTemplate = x.getTaskTemplate
        x.getTask(taskTemplate)
      }
      case _ => throw Exception()

    if cache != null then task.setCache(TransformationCache(cache))
    return task
  }

}
