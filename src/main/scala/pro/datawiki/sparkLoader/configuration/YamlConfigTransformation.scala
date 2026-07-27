package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.exception.ConfigurationException
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.*
import pro.datawiki.sparkLoader.task.Task
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate
import pro.datawiki.yamlConfiguration.LogicClass

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTransformation(objectName: String,
//                                    cache: String,
                                    idMap: YamlConfigTransformationIdMap = null,
                                    sparkSql: YamlConfigTransformationSparkSql = null,
                                    sparkSqlLazy: YamlConfigTransformationSparkSqlLazy = null,
                                    extractSchema: YamlConfigTransformationExtractSchema = null,
                                    extractAndValidateDataFrame: YamlConfigTransformationExtractAndValidateDataFrame = null,
                                    adHoc: YamlConfigTransformationAdHoc = null,
                                    deduplicate: YamlConfigTransformationDeduplicate = null,
                                    sparkGoldenRecord: YamlConfigTransformationSparkGoldenRecord = null
                                   )  {
  @JsonIgnore
  def getLogic: Any = {
    LogicClass.getLogic(idMap, sparkSql, sparkSqlLazy, extractSchema, extractAndValidateDataFrame, adHoc, deduplicate, sparkGoldenRecord)
  }

  @JsonIgnore
  def getTransformation: YamlConfigTransformationTrait = {
    getLogic match
      case x: YamlConfigTransformationTrait => return x
      case _ => throw ConfigurationException(s"Unsupported transformation type: ${this.getClass.getSimpleName}")
  }

  @JsonIgnore
  def createTask(): Task = {
    val task: Task = getLogic match
      case x: YamlConfigTransformationTrait => {
        val taskTemplate: TaskTemplate = x.getTaskTemplate
        x.getTask(taskTemplate)
      }
      case _ => {
        throw ConfigurationException(s"Unsupported transformation type: ${this.getClass.getSimpleName}")
      }

//    if cache != null then task.setCache(TransformationCache(cache),Context.getConnection(cache))
    return task
  }

}
