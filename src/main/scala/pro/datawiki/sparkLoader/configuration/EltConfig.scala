package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.exception.{ConfigurationException, DataProcessingException}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus.error
import pro.datawiki.sparkLoader.task.Task
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.yamlConfiguration.YamlClass

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class EltConfig(
                      connections: List[YamlConfigConnections] = List.apply(),
                      preEtlOperations: List[YamlConfigEltOnServerOperation] = List.apply(),
                      source: List[YamlConfigSource] = List.apply(),
                      transformations: List[YamlConfigTransformation] = List.apply(),
                      target: List[YamlConfigTarget] = List.apply(),
                      postEtlOperations: List[YamlConfigEltOnServerOperation] = List.apply(),
                    ) extends LoggingTrait {

  @JsonIgnore
  def initConnections(): Unit = {
    val startTime = logOperationStart("initialize connections", s"count: ${connections.length}")

    try {
      logInfo(s"Initializing ${connections.length} connections")
      connections.foreach(i => {
        logInfo(s"Initializing connection: ${i.sourceName} (${i.connection})")
        ApplicationContext.setConnection(i.sourceName, ConnectionTrait(i.sourceName, i.connection, i.configLocation))
      })
      logOperationEnd("initialize connections", startTime, s"count: ${connections.length}")

    } catch {
      case e: Exception =>
        logError("initialize connections", e, s"count: ${connections.length}")
        throw e
    }
  }


  @JsonIgnore
  def initPreEtlOperations(): ProgressStatus = {
    val startTime = logOperationStart("initialize pre-ETL operations", s"count: ${preEtlOperations.length}")
    preEtlOperations.foreach(i => {
      i.run("", Map(), true) match {
        case ProgressStatus.done =>
        case ProgressStatus.skip => return ProgressStatus.skip
        case _ => throw Exception()
      }
    })
    logOperationEnd("initialize pre-ETL operations", startTime, s"count: ${preEtlOperations.length}")
    return ProgressStatus.done
  }

  @JsonIgnore
  def initPostEtlOperations(): ProgressStatus = {
    val startTime = logOperationStart("initialize post-ETL operations", s"count: ${preEtlOperations.length}")
    postEtlOperations.foreach(i => {
      i.run("", Map(), true) match {
        case ProgressStatus.done =>
        case ProgressStatus.skip => return ProgressStatus.skip
        case _ => throw Exception()
      }
    })
    logOperationEnd("initialize pre-ETL operations", startTime, s"count: ${preEtlOperations.length}")
    return ProgressStatus.done
  }

  @JsonIgnore
  def initSources(): ProgressStatus = {
    var sourceIsValid: Boolean = true
    var sourceIsEmpty: Boolean = false

    source.foreach(i => {
      try {
        val task: Task = i.createTask()
        task.run(i.objectName, Map(), true) match {
          case ProgressStatus.done =>
          case ProgressStatus.skip =>  return ProgressStatus.skip
          case _ => {
            throw DataProcessingException(s"Source task failed for object: ${i.objectName}")
          }
        }
      } catch {
        case e: Exception => {
          throw DataProcessingException(s"Failed to initialize source: ${i.objectName}", e)
        }
      }
    })
    return ProgressStatus.done
  }

  @JsonIgnore
  def initTransformation(): ProgressStatus = {
    transformations.foreach(i => {
      try {
        val task: Task = i.createTask()
        task.run(i.objectName, Map(), true) match {
          case done =>
          case _ => {
            throw DataProcessingException(s"Transformation task failed for object: ${i.objectName}")
          }
        }
      } catch {
        case e: Exception => {
          throw e
        }
      }
    })
    ProgressStatus.done
  }

  @JsonIgnore
  def runTarget(): ProgressStatus = {
    target.length match
      case 1 => target.head.writeTarget()
      case 0 => throw ConfigurationException("No target configured. At least one target is required.")

      case _ => target.foreach(i => {
        try {
          i.writeTarget()
        } catch {
          case e: Exception => throw DataProcessingException(s"Failed to write target: ${i.getClass.getSimpleName}", e)
        }
      })
        return ProgressStatus.done
  }
}

object EltConfig extends YamlClass {
  def apply(inConfig: String): ProgressStatus = {
    val result = mapper.readValue(getLinesGlobalContext(inConfig), classOf[EltConfig])
    result.initConnections()

    result.initPreEtlOperations() match {
      case ProgressStatus.error => return ProgressStatus.error
      case ProgressStatus.skip => return ProgressStatus.skip
      case ProgressStatus.done =>
    }
    result.initSources() match {
      case ProgressStatus.error => return ProgressStatus.error
      case ProgressStatus.skip => return ProgressStatus.skip
      case ProgressStatus.done =>
    }
    result.initTransformation() match {
      case ProgressStatus.error => return ProgressStatus.error
      case ProgressStatus.skip => return ProgressStatus.skip
      case ProgressStatus.done =>
    }
    result.runTarget() match {
      case ProgressStatus.error => return ProgressStatus.error
      case ProgressStatus.skip => return ProgressStatus.skip
      case ProgressStatus.done =>
    }

    result.initPostEtlOperations() match {
      case ProgressStatus.error => return ProgressStatus.error
      case ProgressStatus.skip => return ProgressStatus.skip
      case ProgressStatus.done => return ProgressStatus.done
      case _ => {
        throw DataProcessingException("Unexpected status from runTarget")
      }
    }
  }
}
