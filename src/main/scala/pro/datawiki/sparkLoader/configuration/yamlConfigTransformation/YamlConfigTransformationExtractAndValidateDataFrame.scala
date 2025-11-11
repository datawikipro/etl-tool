package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.task.{Task, TaskSimple}
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateExtractAndValidateDataFrame}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTransformationExtractAndValidateDataFrame(
                                                                dataFrameIn: String,
                                                                configLocation: String
                                                              ) extends YamlConfigTransformationTrait {
  @JsonIgnore
  override def getTaskTemplate: TaskTemplate = TaskTemplateExtractAndValidateDataFrame(dataFrameIn, configLocation)

  @JsonIgnore
  override def getTask(in: TaskTemplate): Task = TaskSimple(in,false)
}
