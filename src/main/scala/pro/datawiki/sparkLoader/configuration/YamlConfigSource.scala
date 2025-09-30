package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.exception.ConfigurationException
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.*
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.dictionaryEnum.InitModeEnum
import pro.datawiki.sparkLoader.task.*
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateReadEmail}
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.yamlConfiguration.LogicClass

import java.time.LocalDateTime
import java.util.TimeZone

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigSource(
                             sourceName: String,
                             objectName: String,
                             segmentation: String,
                             sourceDb: YamlConfigSourceDBTable,
                             sourceSQL: YamlConfigSourceDBSQL,
                             sourceFileSystem: YamlConfigSourceFileSystem,
                             sourceKafka: YamlConfigSourceKafka,
                             sourceWeb: YamlConfigSourceWeb,
                             sourceMail: YamlConfigSourceMail,
                             sourceBigQuery: YamlConfigSourceBigQuery,
                             initMode: String,
                             skipIfEmpty: Boolean = false
                           ) extends LogicClass {
  @JsonIgnore
  private def initModeEnum: InitModeEnum = InitModeEnum(initMode)

  @JsonIgnore
  private def getLogic: Any = super.getLogic(sourceDb, sourceSQL, sourceFileSystem, sourceKafka, sourceWeb, sourceMail, sourceBigQuery)

  @JsonIgnore
  def createTask(): Task = {
    val taskTemplate: TaskTemplate = getLogic match
      case x: YamlConfigSourceTrait => x.getTaskTemplate(ApplicationContext.getConnection(sourceName))
      case other => throw ConfigurationException(s"Неизвестный тип источника: '$other'. Пожалуйста, проверьте конфигурацию.")

    val task: Task = initModeEnum match
      case InitModeEnum.instantly => TaskSimple(taskTemplate)
      case InitModeEnum.adHoc => TaskAdHocRegister(taskTemplate)
      case InitModeEnum.consumer => {
        val timeZone: TimeZone = TimeZone.getTimeZone("UTC")
        taskTemplate match {
          case x: TaskTemplateReadEmail => {
            //            val today: DateTime = {
            //              val cal = Calendar.getInstance(timeZone)
            //              cal.add(Calendar.DAY_OF_MONTH,0)
            //              cal.set(Calendar.HOUR_OF_DAY, 0)
            //              cal.set(Calendar.MINUTE, 0)
            //              cal.set(Calendar.SECOND, 0)
            //              cal.set(Calendar.MILLISECOND, 0)
            //              cal.getTime
            //            }
            val currentDateTime: LocalDateTime = LocalDateTime.now()
            x.setTime(currentDateTime)

            TaskConsumer(x)
          }
          case other => throw ConfigurationException(s"Неподдерживаемый тип источника данных: '$other'. Проверьте конфигурацию источника.")
        }

      }
      case _ => throw UnsupportedOperationException("Unsupported configuration source case")

    if skipIfEmpty then task.setSkipIfEmpty(skipIfEmpty)
//    if cache != null then task.setCache(TransformationCache(cache),Context.getConnection(sourceName))
    return task
  }

}