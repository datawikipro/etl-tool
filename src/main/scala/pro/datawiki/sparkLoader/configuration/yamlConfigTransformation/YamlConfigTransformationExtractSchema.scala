package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressMode
import pro.datawiki.sparkLoader.task.{Task, TaskSimple}
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateExtractDataFromJsonBySchemaBatch}


@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTransformationExtractSchema(
                                                  tableName: String,
                                                  jsonColumn: String,
                                                  jsonResultColumn: String,
                                                  baseSchema: String,
                                                  mergeSchema: Boolean,
                                                  loadMode: String
                                                ) extends YamlConfigTransformationTrait {
  @JsonIgnore
  override def getTaskTemplate: TaskTemplate = {
    TaskTemplateExtractDataFromJsonBySchemaBatch(
      tableName = tableName,
      jsonColumn = jsonColumn,
      jsonResultColumn = jsonResultColumn,
      baseSchema = baseSchema,
      mergeSchema = mergeSchema,
      loadMode = ProgressMode(loadMode)
    )
  }

  @JsonIgnore
  override def getTask(in: TaskTemplate): Task = TaskSimple(in)
}
