package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.task.*
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateSparkSqlLazy}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTransformationSparkSqlLazy(
                                             sql: String = throw IllegalArgumentException("SQL query is required"),
                                             lazyTable: List[String] = List.empty
                                           ) extends YamlConfigTransformationTrait {
  @JsonIgnore
  override def getTaskTemplate: TaskTemplate = {
    TaskTemplateSparkSqlLazy(sql, lazyTable)
  }

  @JsonIgnore
  override def getTask(in: TaskTemplate): Task = TaskSimple(in)
}
