package pro.datawiki.sparkLoader.configuration

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.exception.ConfigurationException
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.{YamlConfigTargetDatabase, YamlConfigTargetFileSystem, YamlConfigTargetMessageBroker}
import pro.datawiki.yamlConfiguration.LogicClass

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTarget(database: YamlConfigTargetDatabase,
                            fileSystem: YamlConfigTargetFileSystem,
                            messageBroker: YamlConfigTargetMessageBroker,
                            ignoreError: Boolean
                           ) extends LogicClass {
  @JsonIgnore
  def getLogic: YamlConfigTargetTrait = {
    super.getLogic(database, fileSystem,messageBroker) match
      case x: YamlConfigTargetTrait => return x
      case _ => throw new ConfigurationException(s"Unsupported target type: ${this.getClass.getSimpleName}")
  }

  @JsonIgnore
  def writeTarget(): ProgressStatus = {
    if ignoreError then {
      try {
        getLogic.writeTarget()
        return ProgressStatus.done
      } catch
        case e: Exception => {
          println(e.toString)
          return ProgressStatus.skip
        }
    } else {
      getLogic.writeTarget()
      return ProgressStatus.done
    }
  }

}

