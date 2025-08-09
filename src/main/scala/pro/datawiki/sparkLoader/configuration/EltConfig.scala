package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.exception.{ConfigurationException, DataProcessingException}
import pro.datawiki.sparkLoader.configuration.ProgressStatus.error
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.task.{Context, Task}
import pro.datawiki.yamlConfiguration.YamlClass

import scala.collection.mutable

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class EltConfig(
                      connections: List[YamlConfigConnections] = List.apply(),
                      source: List[YamlConfigSource] = List.apply(),
                      transformations: List[YamlConfigTransformation] = List.apply(),
                      target: List[YamlConfigTarget] = List.apply()
                    ) {
  @JsonIgnore
  def initConnections(): Unit = {
    connections.foreach(i => Context.setConnection(i.sourceName, ConnectionTrait(i.connection, i.configLocation)))
  }

  @JsonIgnore
  def initSources(): ProgressStatus = {
    var sourceIsValid: Boolean = true
    var sourceIsEmpty: Boolean = false

    source.foreach(i => {
      try {
        val task: Task = i.createTask()
        task.run(i.objectName, mutable.Map(), true) match {
          case ProgressStatus.done =>
          case ProgressStatus.skip => {
            return ProgressStatus.skip
          }
          case _ => {
            throw new DataProcessingException(s"Source task failed for object: ${i.objectName}")
          }
        }
      } catch {
        case e: Exception => throw new DataProcessingException(s"Failed to initialize source: ${i.objectName}", e)
      }
    })
    return ProgressStatus.done
  }

  @JsonIgnore
  def initTransformation(): ProgressStatus = {
    transformations.foreach(i => {
      try {
        val task: Task = i.createTask()
        task.run(i.getObjectName, mutable.Map(), true) match {
          case done =>
          case _ => {
            throw new DataProcessingException(s"Transformation task failed for object: ${i.getObjectName}")
          }
        }
      } catch {
        case e: Exception => {
          throw new DataProcessingException(s"Failed to initialize transformation: ${i.getObjectName}", e)
        }
        case x: Throwable => {
          throw new DataProcessingException(s"Неизвестная ошибка при инициализации трансформации: ${i.getObjectName}", x)
        }
      }
    })
    ProgressStatus.done
  }

  @JsonIgnore
  def runTarget(): ProgressStatus = {
    target.length match
      case 1 => target.head.writeTarget()
      case 0 => {
        throw new ConfigurationException("No target configured. At least one target is required.")
      }
      case _ => target.foreach(i => {
        try {
          i.writeTarget()
        } catch {
          case e: Exception => throw new DataProcessingException(s"Failed to write target: ${i.getClass.getSimpleName}", e)
        }
      })
        return ProgressStatus.done
  }
}

object EltConfig extends YamlClass {
  def apply(inConfig: String): ProgressStatus = {
    try {
      val result = mapper.readValue(getLines(inConfig), classOf[EltConfig])
      result.initConnections()

      var status: ProgressStatus = ProgressStatus.process

      result.initSources() match {
        case ProgressStatus.error => return ProgressStatus.error
        case ProgressStatus.skip => return ProgressStatus.skip
        case ProgressStatus.done =>
        case _ => throw new DataProcessingException("Unexpected status from initSources")
      }
      result.initTransformation() match {
        case ProgressStatus.error => return ProgressStatus.error
        case ProgressStatus.skip => return ProgressStatus.skip
        case ProgressStatus.done =>
        case _ => throw new DataProcessingException("Unexpected status from initTransformation")
      }

      result.runTarget() match {
        case ProgressStatus.error => return ProgressStatus.error
        case ProgressStatus.skip => return ProgressStatus.skip
        case ProgressStatus.done => return ProgressStatus.done
        case _ => throw new DataProcessingException("Unexpected status from runTarget")
      }
    } catch {
      case e: com.fasterxml.jackson.core.JsonParseException => {
        throw new ConfigurationException(s"Ошибка парсинга YAML конфигурации: ${e.getMessage}", e)
      }
      case e: com.fasterxml.jackson.databind.JsonMappingException => {
        throw new ConfigurationException(s"Ошибка маппинга YAML конфигурации: ${e.getMessage}", e)
      }
      case e: ConfigurationException => {
        throw e // Пробрасываем существующие ошибки конфигурации
      }
      case e: DataProcessingException => {
        throw e // Пробрасываем существующие ошибки обработки данных
      }
      case e: Exception => {
        throw new ConfigurationException(s"Непредвиденная ошибка при обработке конфигурации ETL: ${e.getMessage}", e)
      }
    }
  }
}
