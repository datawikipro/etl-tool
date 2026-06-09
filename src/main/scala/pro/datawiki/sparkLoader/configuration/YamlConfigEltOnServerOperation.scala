package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.exception.{ConfigurationException, DataProcessingException}
import pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation.{YamlConfigEltOnServerColdDataFileBased, YamlConfigEltOnServerSQL}
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus
import pro.datawiki.sparkLoader.task.{Task, TaskSimple}
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.yamlConfiguration.LogicClass

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigEltOnServerOperation(
                                           eltOnServerOperationName: String,
                                           sourceName: String,
                                           sql: YamlConfigEltOnServerSQL,
                                           coldDataFileBased: YamlConfigEltOnServerColdDataFileBased,
                                           ignoreError: Boolean
                                         ) extends LoggingTrait {
  @JsonIgnore
  def getLogic: YamlConfigEltOnServerOperationTrait = {
    LogicClass.getLogic(sql, coldDataFileBased) match
      case x: YamlConfigEltOnServerOperationTrait => return x
      case _ => throw ConfigurationException(s"Unsupported target type: ${this.getClass.getSimpleName}")
  }

  def createTask(): Task = {
    val taskTemplate: TaskTemplate = getLogic match {
      case x: YamlConfigEltOnServerOperationTrait => x.getTaskTemplate(ApplicationContext.getConnection(sourceName))
      case other => throw ConfigurationException(s"Неизвестный тип источника: '$other'. Пожалуйста, проверьте конфигурацию.")
    }
    return TaskSimple(taskTemplate, false)
  }

  def run(targetName: String, parameters: Map[String, String], isSync: Boolean): ProgressStatus = {
    logInfo(s"Executing out-ETL operation: ${eltOnServerOperationName}")
    try {
      val task = createTask()
      task.run(targetName, parameters, isSync) match {
        case ProgressStatus.done => {
          logInfo(s"Pre-ETL operation completed: ${eltOnServerOperationName}")
          return ProgressStatus.done
        }
        case ProgressStatus.skip => {
          logInfo(s"Pre-ETL operation skipped: ${eltOnServerOperationName}")
          return ProgressStatus.skip
        }
        case _ => {
          logError("pre-ETL operation", DataProcessingException(s"PreEtlOperations task failed for object: ${eltOnServerOperationName}"))
          throw DataProcessingException(s"PreEtlOperations task failed for object: ${eltOnServerOperationName}")
        }
      }
    } catch {
      case e: Exception =>
        throw e
    }
  }

}

