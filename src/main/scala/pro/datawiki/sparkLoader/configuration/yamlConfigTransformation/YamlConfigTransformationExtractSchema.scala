package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.dictionaryEnum.ProgressMode
import pro.datawiki.sparkLoader.task.{Task, TaskSimple}
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateExtractDataFromJsonBySchemaBatch}


@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTransformationExtractSchema(
                                                  tableName: String = throw Exception("a"),
                                                  jsonColumn: String = throw Exception("b"),
                                                  jsonResultColumn: String = throw Exception("c"),
                                                  baseSchema: String = throw Exception("d"),
                                                  mergeSchema: Boolean = throw Exception("e"),
                                                  loadMode: String = throw Exception("f")
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
