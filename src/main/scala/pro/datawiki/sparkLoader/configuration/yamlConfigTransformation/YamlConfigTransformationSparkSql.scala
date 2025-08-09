package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.task.{Task, TaskSimple, TaskTemplate, TaskTemplateSparkSql}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTransformationSparkSql(
                                             sql: String,
                                             isLazyTransform: Boolean = false,
                                             lazyTable: List[String] = List.empty
                                           ) extends YamlConfigTransformationTrait {
  @JsonIgnore
  override def getTaskTemplate: TaskTemplate = TaskTemplateSparkSql(sql,isLazyTransform,lazyTable)
  @JsonIgnore
  override def getTask(in: TaskTemplate): Task = TaskSimple(in)
}
