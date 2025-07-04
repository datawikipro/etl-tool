package pro.datawiki.sparkLoader.configuration

import java.time.LocalDateTime
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.*
import pro.datawiki.sparkLoader.task.*
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.yamlConfiguration.LogicClass

import java.util.{Calendar, Date, TimeZone}
import scala.concurrent.ExecutionContext


class YamlConfigSource(sourceName: String,
                       objectName: String,
                       segmentation: String,
                       sourceDb: YamlConfigSourceDBTable,
                       sourceSQL: YamlConfigSourceDBSQL,
                       sourceFileSystem: YamlConfigSourceFileSystem,
                       sourceKafka: YamlConfigSourceKafka,
                       sourceWeb: YamlConfigSourceWeb,
                       sourceMail: YamlConfigSourceMail,
                       cache: String,
                       initMode: String,
                       skipIfEmpty: Boolean = false
                      ) extends LogicClass {
  private def initModeEnum: InitModeEnum = InitModeEnum(initMode)


  def getObjectName: String = objectName

  private def getLogic: Any = {
    super.getLogic(sourceDb, sourceSQL, sourceFileSystem, sourceKafka, sourceWeb,sourceMail)
  }

  def createTask(): Task = {
    val taskTemplate: TaskTemplate = getLogic match
      case x: YamlConfigSourceTrait => x.getTaskTemplate(Context.getConnection(sourceName))
      case _ => throw Exception()

    val task: Task = initModeEnum match
      case InitModeEnum.instantly => TaskSimple(taskTemplate)
      case InitModeEnum.adHoc => TaskAdHocRegister(taskTemplate)
      case InitModeEnum.consumer => {
        val timeZone: TimeZone = TimeZone.getTimeZone("UTC")
        taskTemplate match {
          case x: TaskTemplateReadEmail =>{
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
          case _ => throw Exception()
        }

      }
      case _ => throw Exception()

    if skipIfEmpty then task.setSkipIfEmpty(skipIfEmpty)
    if cache != null then task.setCache(TransformationCache(cache))
    return task
  }

}