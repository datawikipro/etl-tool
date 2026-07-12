package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.context.SparkContext
import pro.datawiki.sparkLoader.task.{Task, TaskSimple}
import pro.datawiki.sparkLoader.taskTemplate.TaskTemplate

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class YamlConfigTransformationDeduplicate(
                                                sourceTable: String = throw new IllegalArgumentException("sourceTable is required"),
                                                uniqueKey: List[String] = throw new IllegalArgumentException("uniqueKey is required"),
                                                deduplicationKey: List[String] = throw new IllegalArgumentException("deduplicationKey is required")
                                              ) extends YamlConfigTransformationTrait {
  
  @JsonIgnore
  override def getTaskTemplate: TaskTemplate = {
    new TaskTemplate {
      override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
        val startTime = logOperationStart("Spark SQL Deduplicate execution", s"Deduplicating $sourceTable")
        
        try {
          SparkContext.initTables()
          
          var df = SparkObject.spark.table(sourceTable)
          var updated = false
          uniqueKey.foreach { keyCol =>
            if (!df.columns.contains(keyCol)) {
              df = df.withColumn(keyCol, org.apache.spark.sql.functions.lit(null).cast("string"))
              updated = true
            }
          }
          if (updated) {
            df.createOrReplaceTempView(sourceTable)
          }

          val sql = 
            s"""with deduplicated as (
               |  select *, 
               |         row_number() over (partition by ${uniqueKey.mkString(", ")} order by ${deduplicationKey.mkString(", ")}) as __tmp_rn
               |    from $sourceTable
               |)
               |select * 
               |  from deduplicated 
               | where __tmp_rn = 1""".stripMargin
               
          logInfo(s"Executing SQL for Deduplicate: $sql")
          df = SparkObject.spark.sql(sql).drop("__tmp_rn")
          LogMode.debugDF(df)
          logOperationEnd("Spark SQL Deduplicate execution", startTime, "successful")
          
          List(new DataFrameOriginal(df))
        } catch {
          case e: Exception =>
            logError("Spark SQL Deduplicate execution failed", e, s"source: $sourceTable")
            throw e
        }
      }
    }
  }

  @JsonIgnore
  override def getTask(in: TaskTemplate): Task = TaskSimple(in, false)
}
