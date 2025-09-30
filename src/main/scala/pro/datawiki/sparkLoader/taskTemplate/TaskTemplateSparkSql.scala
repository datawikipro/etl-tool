package pro.datawiki.sparkLoader.taskTemplate

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.context.SparkContext
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

class TaskTemplateSparkSql(sql: String) extends TaskTemplate with LoggingTrait {

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val startTime = logOperationStart("Spark SQL execution", s"sql: ${sql.take(100)}...")

    try {
      SparkObject.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

      logInfo("Executing immediate SQL transformation")
      val df: DataFrame = try {
        SparkContext.initTables()
        SparkObject.spark.sql(sql)
      } catch {
        case e: Exception => {
          throw e
        }
      }
      logInfo(s"Executing SQL: $sql")
      LogMode.debugDF(df)
      logOperationEnd("Spark SQL execution", startTime, "immediate execution")
      return List.apply(new DataFrameOriginal(df))

    } catch {
      case e: Exception =>
        logError("Spark SQL execution", e, s"sql: ${sql.take(50)}...")
        throw e
    }
  }

}
