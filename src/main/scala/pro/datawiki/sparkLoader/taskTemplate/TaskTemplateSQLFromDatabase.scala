package pro.datawiki.sparkLoader.taskTemplate

import org.apache.spark.sql.{Column, DataFrame}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}

import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import scala.collection.mutable

class TaskTemplateSQLFromDatabase(sql: String,
                                  connection: ConnectionTrait) extends TaskTemplate with LoggingTrait {

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val startTime = logOperationStart("SQL from database", s"sql: ${sql.take(50)}...")

    try {
      logInfo(s"Executing SQL query from database")
      logConfigInfo("SQL query", s"sql: ${sql.take(100)}..., sync: $isSync")

      connection match
        case x: DatabaseTrait =>
          logInfo("Using DatabaseTrait connection for SQL execution")
          val sparkDf = x.getDataFrameBySQL(s"""${sql}""".stripMargin)
          val df = DataFrameOriginal(sparkDf)
          logOperationEnd("SQL from database", startTime, s"sql: ${sql.take(50)}...")
          return List.apply(df)
        case _ =>
          logError("SQL from database", pro.datawiki.exception.UnsupportedOperationException("Unsupported connection type for SQL execution"))
          throw UnsupportedOperationException("Unsupported connection type for SQL execution")

    } catch {
      case e: Exception =>
        logError("SQL from database", e, s"sql: ${sql.take(50)}...")
        throw e
    }
  }

}