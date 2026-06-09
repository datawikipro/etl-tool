package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.task.*
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateSparkSql}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTransformationSparkDeduplicate(tableName: String,
                                                    uniqueKey: List[String],
                                                    deduplicateColumn: List[String]) extends YamlConfigTransformationTrait {
  @JsonIgnore
  override def getTaskTemplate: TaskTemplate = {
    return TaskTemplateSparkSql(
      s"""with calc as (
         |select *,
         |       row_number() over (partition by ${deduplicateColumn.mkString(", ")} order by ${deduplicateColumn.mkString(",")}) as rn
         |  from $tableName)
         |select * from calc where rn = 1""".stripMargin.strip())
  }

  @JsonIgnore
  override def getTask(in: TaskTemplate): Task = TaskSimple(in,false)
}
