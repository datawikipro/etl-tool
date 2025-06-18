package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.task.{Context, Task, TaskTemplate}
import pro.datawiki.sparkLoader.transformation.TransformationIdMap
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

class EltConfig(
                 connections: List[YamlConfigConnections] = List.apply(),
                 source: List[YamlConfigSource] = List.apply(),
                 idmap: String,
                 transformations: List[YamlConfigTransformation] = List.apply(),
                 target: List[YamlConfigTarget] = List.apply()
               ) {

  def initConnections(): Unit = {
    connections.foreach(i => Context.setConnection(i.sourceName, ConnectionTrait.initConnection(i.connection, i.configLocation)))
  }

  def runTask(taskId: String): Boolean = {
    val task: TaskTemplate = Context.getTaskTemplate(taskId)
    //    return task.run()
    throw Exception()
  }

  def initSources(): Unit = {
    getSegmentation match
      case SegmentationEnum.full => {
        source.foreach(i => {
          val task: Task = i.createTask()
          task.run(i.getObjectName, mutable.Map(), true)
        })
      }
      case SegmentationEnum.random =>
        throw Exception()
      case _ => throw Exception()
  }

  def initTransformation(): Unit = {
    //     transformations.foreach(i => run(i.getObjectName, i.getTransformation))
    transformations.foreach(i => {
      val task: Task = i.createTask()
      task.run(i.getObjectName, mutable.Map(), true)
    })
  }

  var segmentation: SegmentationEnum = SegmentationEnum.full

  def init(): Unit = {
    source.foreach(i => {
      i.getSegmentation match
        case SegmentationEnum.full =>
        case SegmentationEnum.partition => segmentation = SegmentationEnum.partition
        case SegmentationEnum.random =>
          segmentation match
            case SegmentationEnum.full => segmentation = SegmentationEnum.random
            case SegmentationEnum.random => throw Exception()
            case _ => throw Exception()
        case SegmentationEnum.adHoc =>
        case _ => throw Exception()
    })
  }

  def getSegmentation: SegmentationEnum = {
    return segmentation
  }

  def getIdmapSource: String = {
    return idmap
  }

  def runTarget(): Boolean = {
    target.length match
      case 1 => target.head.writeTarget()
      case 0 => {
        throw Exception()
      }
      case _ => target.foreach(i => {
        i.writeTarget()
      })
      return true
  }
}

object EltConfig extends YamlClass {
  def apply(inConfig: String): Boolean = {
    val result = mapper.readValue(getLines(inConfig), classOf[EltConfig])
    result.init()
    result.initConnections()
    TransformationIdMap.setIdmap(result.getIdmapSource)

    result.initSources()
    result.initTransformation()
    
    result.runTarget()
  }
}
