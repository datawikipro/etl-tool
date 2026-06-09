package pro.datawiki.sparkLoader.taskTemplate

import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.traits.LoggingTrait

class TaskTemplateSQLFromDatabase(sql: String,
                                  connection: ConnectionTrait) extends TaskTemplate with TaskTemplateRunAtServerRegister with LoggingTrait {

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val startTime = logOperationStart("SQL from database", s"sql: ${sql.take(50)}...")

    try {
      logInfo(s"Executing SQL query from database")
      logConfigInfo("SQL query", s"sql: ${sql.take(100)}..., sync: $isSync")
      connection match
        case x: DatabaseTrait =>
          val sparkDf = x.getDataFrameBySQL(s"""${sql}""".stripMargin)
          val df = DataFrameOriginal(sparkDf)
          return List.apply(df)
        case _ =>
          throw UnsupportedOperationException("Unsupported connection type for SQL execution")
    } catch {
      case e: Exception =>
        logError("SQL from database", e, s"sql: ${sql.take(50)}...")
        throw e
    } finally {
      logOperationEnd("SQL from database", startTime, s"sql: ${sql.take(50)}...")
    }
  }


  override def getAtServerCode(targetTable: String, parameters: Map[String, String], isSync: Boolean): Boolean = {
    connection match {
      case x: DatabaseTrait => {
        return x.setTemporaryTable(tableName = targetTable, sql = sql)
      }
      case fs => {
        throw Exception()
      }
    }
  }
}