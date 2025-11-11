package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.exception.ConfigurationException
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.{YamlConfigTargetDatabase, YamlConfigTargetDummy, YamlConfigTargetFileSystem, YamlConfigTargetMessageBroker}
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressStatus
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.yamlConfiguration.LogicClass

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTarget(database: YamlConfigTargetDatabase,
                            fileSystem: YamlConfigTargetFileSystem,
                            messageBroker: YamlConfigTargetMessageBroker,
                            dummy: YamlConfigTargetDummy,
                            ignoreError: Boolean
                           ) extends  LoggingTrait {
  @JsonIgnore
  def getLogic: YamlConfigTargetTrait = {
    LogicClass.getLogic(database, fileSystem, messageBroker, dummy) match
      case x: YamlConfigTargetTrait => return x
      case _ => throw ConfigurationException(s"Unsupported target type: ${this.getClass.getSimpleName}")
  }

  @JsonIgnore
  def writeTarget(): ProgressStatus = {
    try {
      getLogic.writeTarget()
      return ProgressStatus.done
    } catch {
      case e: org.apache.kafka.common.errors.UnknownTopicOrPartitionException => {
        logError("Read topic", e)
        return ProgressStatus.skip
      }
      case e: Exception => {
        if ignoreError then {
          println(e.toString)
          return ProgressStatus.skip
        }

        throw e
      }
    }


  }

}

