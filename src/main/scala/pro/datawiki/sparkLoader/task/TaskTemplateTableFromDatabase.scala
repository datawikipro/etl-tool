package pro.datawiki.sparkLoader.task

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameLazyDatabase, DataFrameOriginal, DataFramePartition, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.{YamlConfigSourceDBTableColumn, YamlConfigSourceDBTablePartition}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}

import scala.collection.mutable

class TaskTemplateTableFromDatabase(tableSchema: String,
                                    tableName: String,
                                    tableColumns: List[YamlConfigSourceDBTableColumn] = List.apply(),
                                    partitionBy: List[YamlConfigSourceDBTablePartition] = List.apply(),
                                    filter: String,
                                    limit: Int,
                                    connection: ConnectionTrait) extends TaskTemplate {
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
      case false => getColumnNames.mkString(",")
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

  private def getTable(src: ConnectionTrait, parameters: mutable.Map[String, String]): DataFrame = {
    var df: DataFrame = null
    var sql =
      s"""select ${getSQLColumnList}
         |  from ${tableSchema}.${tableName}
         |  $getSQLWhere
         |  $getSQLLimit
         |  """.stripMargin
    parameters.foreach(i => {
      sql = sql.replace(s"$${${i._1}}", i._2)
    })
    src match
      case x: DatabaseTrait =>
        return x.getDataFrameBySQL(sql)
      case _ => throw Exception()
  }

  private def getPartitions(src: ConnectionTrait): List[String] = {
    src match
      case x: DatabaseTrait =>
        return x.getPartitionsForTable(s"${tableSchema}.${tableName}")
      case _ => throw Exception()
  }

  private def getTablePartition(connection: DatabaseTrait, partitionName: String): DataFrameTrait = {
    val sql =
      s"""select ${getSQLColumnList}
         |  from ${partitionName}
         |  $getSQLWhere
         |  """.stripMargin
    return DataFrameLazyDatabase(connection, sql)
  }


  override def run(parameters: mutable.Map[String, String], isSync: Boolean): List[DataFrameTrait] = {
    if partitionBy.isEmpty then {
      return List.apply(DataFrameOriginal(getTable(src = connection, parameters = parameters)))
    } else {
      var list: mutable.Map[String,DataFrameTrait] = mutable.Map() 
      getPartitions(src = connection).foreach(i => {
      connection match
        case x: DatabaseTrait =>
          list += (i-> getTablePartition(connection = x, partitionName = i))
        case _ => throw Exception()
      })
      return List.apply(DataFramePartition(list))
    }
  }

}
