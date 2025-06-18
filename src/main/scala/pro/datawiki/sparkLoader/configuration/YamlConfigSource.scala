package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.configuration.SegmentationEnum.{full, random}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.*
import pro.datawiki.sparkLoader.task.*
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.yamlConfiguration.LogicClass

import scala.concurrent.ExecutionContext


class YamlConfigSource(sourceName: String,
                       objectName: String,
                       segmentation: String,
                       sourceDb: YamlConfigSourceDBTable,
                       sourceSQL: YamlConfigSourceDBSQL,
                       sourceFileSystem: YamlConfigSourceFileSystem,
                       sourceKafka: YamlConfigSourceKafka,
                       sourceWeb: YamlConfigSourceWeb,
                       cache: String,
                       initMode: String
                      ) extends LogicClass {
  val initModeEnum: InitModeEnum = initMode match
    case "instantly" => InitModeEnum.instantly
    case "adHoc" => InitModeEnum.adHoc
    case _ => throw Exception("initMode not defined")

  def getObjectName: String = objectName

  def getSegmentation: SegmentationEnum = {
    if segmentation == null then {
      return SegmentationEnum.full
    }
    segmentation match
      case "random" => return SegmentationEnum.random
      case "full" => return SegmentationEnum.full
      case "partition" => return SegmentationEnum.partition
      case _ => throw Exception()
  }

  private def getLogic: Any = {
    super.getLogic(sourceDb, sourceSQL, sourceFileSystem, sourceKafka, sourceWeb)
  }

  private def getSource: YamlConfigSourceTrait = {
    getLogic match
      case x: YamlConfigSourceTrait => return x
      case _ => throw Exception()
  }

  def createTask(): Task = {
    val taskTemplate: TaskTemplate = getLogic match
      case x: YamlConfigSourceTrait => x.getTaskTemplate(Context.getConnection(sourceName))
      case _ => throw Exception()


    val task: Task = initModeEnum match
      case InitModeEnum.instantly => TaskSimple(taskTemplate)
      case InitModeEnum.adHoc => TaskAdHocRegister(taskTemplate)
      case _ => throw Exception()

    if cache != null then task.setCache(TransformationCache(cache))
    return task
  }

}