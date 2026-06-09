package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.{DataFrameLazySparkSql, DataFramePartition, DataFrameTrait}
import pro.datawiki.exception.ConfigurationException
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.context.SparkContext
import pro.datawiki.sparkLoader.traits.LoggingTrait

import scala.collection.mutable

class TaskTemplateSparkSqlLazy(sql: String,
                               lazyTable: List[String] = List.empty) extends TaskTemplate with LoggingTrait {

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val startTime = logOperationStart("Spark SQL execution", s"sql: ${sql.take(100)}..., tables: ${lazyTable.mkString(",")}")

    try {
      SparkObject.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

      logInfo("Executing lazy SQL transformation")
      lazyTable.length match
        case 0 =>
          logInfo("Creating lazy DataFrame without table dependencies")
          logOperationEnd("Spark SQL execution", startTime, "lazy execution without tables")
          return List.apply(DataFrameLazySparkSql(sql, mutable.Map()))
        case 1 => {
          logInfo(s"Creating lazy DataFrame with table dependency: ${lazyTable.head}")
          var list: mutable.Map[String, DataFrameTrait] = mutable.Map()
          val viewName = lazyTable.head
          val view = SparkContext.getView(viewName)
          view match
            case x: DataFramePartition => {
              logInfo(s"Processing DataFramePartition with ${x.getPartitions.size} partitions")
              x.getPartitions.foreach(i => {
                list += (i._1 -> DataFrameLazySparkSql(sql = sql, inInitTables = mutable.Map((viewName -> i._2))))
              })
            }
            case _ =>
              logError("lazy transformation", ConfigurationException("View type not supported"))
              throw ConfigurationException("View type not supported")
          logOperationEnd("Spark SQL execution", startTime, s"lazy execution with ${list.size} partitions")
          return List.apply(DataFramePartition(list))
        }
        case _ =>
          logError("lazy transformation", ConfigurationException("Invalid lazyTable format"))
          throw ConfigurationException("Invalid lazyTable format")

    } catch {
      case e: Exception =>
        logError("Spark SQL execution", e, s"sql: ${sql.take(50)}...")
        throw e
    }
  }
  
}
