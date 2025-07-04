package pro.datawiki.sparkLoader.configuration

import pro.datawiki.sparkLoader.configuration.ProgressStatus.error
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.task.{Context, Task}
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

class EltConfig(
                 connections: List[YamlConfigConnections] = List.apply(),
                 source: List[YamlConfigSource] = List.apply(),
                 transformations: List[YamlConfigTransformation] = List.apply(),
                 target: List[YamlConfigTarget] = List.apply()
               ) {

  def initConnections(): Unit = {
    connections.foreach(i => Context.setConnection(i.sourceName, ConnectionTrait.initConnection(i.connection, i.configLocation)))
  }

  def initSources(): ProgressStatus = {
    var sourceIsValid: Boolean = true
    var sourceIsEmpty: Boolean = false

    source.foreach(i => {
      val task: Task = i.createTask()
      task.run(i.getObjectName, mutable.Map(), true) match {
        case ProgressStatus.done =>
        case ProgressStatus.skip => return ProgressStatus.skip
        case _ => {
          throw Exception()
        }
      }
    })
    return ProgressStatus.done
  }

  def initTransformation(): ProgressStatus = {
    transformations.foreach(i => {
      val task: Task = i.createTask()
      task.run(i.getObjectName, mutable.Map(), true) match {
        case done =>
        case _ => throw Exception()
      }
    })
    ProgressStatus.done
  }

  def runTarget(): ProgressStatus = {
    target.length match
      case 1 => target.head.writeTarget()
      case 0 => {
        throw Exception()
      }
      case _ => target.foreach(i => {
        i.writeTarget()
      })
        return ProgressStatus.done
  }
}

object EltConfig extends YamlClass {
  def apply(inConfig: String): ProgressStatus = {
    val result = mapper.readValue(getLines(inConfig), classOf[EltConfig])
    result.initConnections()

    var status: ProgressStatus = ProgressStatus.process

    result.initSources() match  {
      case ProgressStatus.error => return ProgressStatus.error
      case ProgressStatus.skip => return ProgressStatus.skip
      case ProgressStatus.done =>
      case _=> throw Exception()
    }
    result.initTransformation() match  {
      case ProgressStatus.error => return ProgressStatus.error
      case ProgressStatus.skip => return ProgressStatus.skip
      case ProgressStatus.done =>
      case _=> throw Exception()
    }

    result.runTarget() match  {
      case ProgressStatus.error => return ProgressStatus.error
      case ProgressStatus.skip => return ProgressStatus.skip
      case ProgressStatus.done => return ProgressStatus.done
      case _=> throw Exception()
    }
  }
}
