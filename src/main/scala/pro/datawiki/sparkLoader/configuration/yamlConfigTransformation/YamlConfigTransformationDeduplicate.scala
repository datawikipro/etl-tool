package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.task.{Task, TaskSimple}
import pro.datawiki.sparkLoader.taskTemplate.{TaskTemplate, TaskTemplateSparkSql}

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTransformationDeduplicate(
                                                sourceTable: String = throw IllegalArgumentException("sourceTable is required"),
                                                uniqueKey: List[String] = throw IllegalArgumentException("uniqueKey is required"),
                                                deduplicationKey: List[String] = throw IllegalArgumentException("deduplicationKey is required")
                                              ) extends YamlConfigTransformationTrait {
  
  @JsonIgnore
  override def getTaskTemplate: TaskTemplate = {
    val sql = 
      s"""with deduplicated as (
         |  select *, 
         |         row_number() over (partition by ${uniqueKey.mkString(", ")} order by ${deduplicationKey.mkString(", ")}) as rn
         |    from $sourceTable
         |)
         |select * 
         |  from deduplicated 
         | where rn = 1""".stripMargin
    
    return TaskTemplateSparkSql(sql)
  }

  @JsonIgnore
  override def getTask(in: TaskTemplate): Task = TaskSimple(in)
}
