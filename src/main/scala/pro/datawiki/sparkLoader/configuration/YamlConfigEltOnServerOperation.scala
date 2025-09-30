package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.exception.ConfigurationException
import pro.datawiki.sparkLoader.configuration.yamlConfigEltOnServerOperation.YamlConfigEltOnServerSQL
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.task.{Task, TaskSimple}
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate
import pro.datawiki.yamlConfiguration.LogicClass

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigEltOnServerOperation(
                                           eltOnServerOperationName: String,
                                           sourceName: String,
                                           sql: YamlConfigEltOnServerSQL,
                                           ignoreError: Boolean
                                         ) extends LogicClass {
  @JsonIgnore
  def getLogic: YamlConfigEltOnServerOperationTrait = {
    super.getLogic(sql) match
      case x: YamlConfigEltOnServerOperationTrait => return x
      case _ => throw ConfigurationException(s"Unsupported target type: ${this.getClass.getSimpleName}")
  }

  def createTask(): Task = {
    val taskTemplate: TaskTemplate = getLogic match {
      case x: YamlConfigEltOnServerOperationTrait => x.getTaskTemplate(ApplicationContext.getConnection(sourceName))
      case other => throw ConfigurationException(s"Неизвестный тип источника: '$other'. Пожалуйста, проверьте конфигурацию.")
    }
    return TaskSimple(taskTemplate)
  }

}

