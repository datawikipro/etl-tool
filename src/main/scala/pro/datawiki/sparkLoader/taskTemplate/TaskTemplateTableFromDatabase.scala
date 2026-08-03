package pro.datawiki.sparkLoader.taskTemplate

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameEmpty, DataFrameLazyDatabase, DataFrameOriginal, DataFramePartition, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, NoSQLDatabaseTrait}
import pro.datawiki.sparkLoader.traits.LoggingTrait

import scala.collection.mutable

class TaskTemplateTableFromDatabase(tableSchema: String,
                                    tableName: String,
                                    tableColumns: List[YamlConfigSourceDBTableColumn] = List.apply(),
                                    filter: String,
                                    limit: Int,
                                    connection: ConnectionTrait) extends TaskTemplate  with TaskTemplateRunAtServerRegister with LoggingTrait {
  private def getColumnNames: List[String] = {
    var lst: List[String] = List.empty
    tableColumns.foreach(i =>
      lst = lst.appended(i.columnName)
    )
    return lst
  }

  private def getSQLColumnList: String = {
    getColumnNames.isEmpty match
      case true => "*"
      case false => getColumnNames.map(col => s"$col").mkString(",")
  }

  private def getSQLWhere: String = {
    filter match
      case null => ""
      case _ => s"where $filter"
  }

  private def getSQLLimit: String = {
    limit match
      case 0 => ""
      case _ => s"limit $limit"
  }

  private def getFullTableName: String = {
    if (tableSchema == null || tableSchema.trim.isEmpty || tableSchema == "null") tableName
    else s"${tableSchema}.${tableName}"
  }

  private def getReadSql(parameters: Map[String, String]): String = {
    var sql = s"""select ${getSQLColumnList}
       |  from ${getFullTableName}
       |  $getSQLWhere
       |  """.stripMargin
    parameters.foreach(i => {
      sql = sql.replace(s"$${${i._1}}", i._2)
    })
    sql
  }

  private def getTable(src: ConnectionTrait, parameters: Map[String, String]): DataFrameTrait = {
    var df: DataFrame = null
    src match
      case x: DatabaseTrait => {
        val rawSql = getReadSql(parameters)
        val finalSql = x.applyLimitToSql(rawSql, limit)
        val df = x.getDataFrameBySQL(finalSql)
        if df.count() == 0 then return DataFrameEmpty()
        return DataFrameOriginal(df)
      }
      case x: NoSQLDatabaseTrait =>
        return x.readDf(s"${tableName}")
      case _ => throw UnsupportedOperationException("Unsupported connection type for table reading")
  }

  override def run(parameters: Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    val startTime = logOperationStart("database table load", s"schema: $tableSchema, table: $tableName")

    try {
      logInfo(s"Loading data from database table: ${getFullTableName}")
      logConfigInfo("database table", s"columns: ${tableColumns.length}, filter: $filter, limit: $limit")
      
      val df = getTable(src = connection, parameters = parameters)
      logOperationEnd("database table load", startTime, s"table: $tableName")
      return List.apply(df)

    } catch {
      case e: Exception =>
        logError("database table load", e, s"table: $tableSchema.$tableName")
        throw e
    }
  }

  override def getAtServerCode(targetTable: String, parameters: Map[String, String], isSync: Boolean): Boolean = {
    connection match {
      case x: DatabaseTrait => {
        return x.setTemporaryTable(tableName = targetTable, sql = getReadSql(parameters))
      }
      case fs => {
        throw Exception()
      }
    }
  }
}
