package pro.datawiki.sparkLoader.connection.postgres

import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.dictionaryEnum.SCDType

import java.sql.Timestamp
import scala.util.Random

class TableSqlGenerate(
                        tableSchema: String,
                        tableName: String,
                        scdType: SCDType,
                        columns: List[String],
                        uniqueKey: List[String],
                        partitionBy: List[(String, String)],
                        dataBase: LoaderPostgres
                      ) {
  var aliasName = ""

  private def systemColumns: List[(String, String)] = {
    scdType match {
      case SCDType.SCD_3 => {
        return List(
          ("valid_from_dttm", s"cast('$startTime' as TimeStamp)"),
          ("valid_to_dttm", "to_date('2100','yyyy')"),
          ("run_id", s"""'${ApplicationContext.getRunId}'""")
        )
      }
      case fs => {
        throw Exception()
      }
    }

  }

  def getColumnsWithoutChanges: List[String] = {
    dataBase.getTableColumns(tableSchema, tableName)
      .map(col => col.column_name)
      .filter(col => !uniqueKey.contains(col) && !getColumnsWithChanges.contains(col) && !systemColumns.map(col => col._1).contains(col))
  }

  val startTime: Timestamp = new Timestamp(System.currentTimeMillis())

  def getTableSchema: String = tableSchema

  def getTableName: String = tableName

  def getSystemColumnKey: List[String] = systemColumns.map(col => col._1)

  def getColumnKey: List[String] = {
    return columns ::: getSystemColumnKey
  }

  def getSystemColumnValue: List[String] = systemColumns.map(col => s"${col._2} as ${col._1}")

  def getColumnValue: List[String] = {
    return columns ::: getSystemColumnValue
  }

  def getUniqueKey: List[String] = {
    if uniqueKey.isEmpty then return List.empty
    return uniqueKey ::: List("valid_to_dttm")
  }

  def getColumnsWithChanges: List[String] = {
    return columns.filter(col => !uniqueKey.toSet.contains(col))
  }

  def getTableAlias: String = {
    if aliasName == "" then throw Exception()
    return aliasName
  }

  def initTableAlias(tmpSchemaName: String): Boolean = {
    aliasName = s"$tmpSchemaName.${tableName}__${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
    var sql =s"""create view $aliasName as select * from $tableSchema.$tableName where true"""
    sql = partitionBy.length match {
      case 0 =>   sql
      case _ =>  s"""$sql and ${partitionBy.map(col => s"""${col._1} = '${col._2}'""").mkString(" and ")}"""
    }
    sql = scdType match {
      case SCDType.SCD_3 => s"""$sql and valid_to_dttm = to_date('2100','yyyy')"""
      case _=> {
        throw Exception()
      }
    }
    dataBase.runSQL(sql)
    return true
  }
}
