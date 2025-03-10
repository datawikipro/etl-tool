package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.configuration.{RunConfig, YamlConfigSourceTrait}
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, DatabaseTrait}

case class YamlConfigSourceDBTable(
                                    tableSchema: String,
                                    tableName: String,
                                    tableColumns: List[YamlConfigSourceDBTableColumn] = List.apply(),
                                    partitionKey: String,
                                    filter: String,
                                    limit: Int) extends YamlConfigSourceTrait {
  private def getColumnNames: List[String] = {
    var lst: List[String] = List.empty
    tableColumns.foreach(i =>
      lst = lst.appended(i.columnName)
    )
    return lst
  }

  private def getSQLColumnList:String = {
    getColumnNames.isEmpty match
      case true => "*"
      case false =>getColumnNames.mkString(",")
  }

  private def getSQLWhere:String = {
    filter match
      case null => ""
      case _ => s"where $filter"
  }

  private def getSQLLimit: String = {
    limit match
      case 0 => ""
      case _ => s"limit $limit"
  }

  private def getTable(sourceName: String): DataFrame = {
    val src = Connection.getConnection(sourceName)
    var df: DataFrame = null
    src match
      case x: DatabaseTrait =>
            return x.getDataFrameBySQL(
              s"""select ${getSQLColumnList}
                 |  from ${tableSchema}.${tableName}
                 |  $getSQLWhere
                 |  $getSQLLimit
                 |  """.stripMargin)
      case _ => throw Exception()
  }

  private def getTablePartition(sourceName: String): DataFrame = {
    val src = Connection.getConnection(sourceName)

    var df: DataFrame = null
    src match
      case x: DatabaseTrait => {
        getColumnNames.isEmpty match
          case false => return x.getDataFrameBySQL(
            s"""select ${getColumnNames.mkString(",")}
               |  from ${tableSchema}.${tableName}
               | where $partitionKey = ${RunConfig.getPartition}""".stripMargin)
          case true => return x.getDataFrameBySQL(
            s"""select *
               |  from ${tableSchema}.${tableName}
               | where $partitionKey = ${RunConfig.getPartition}""".stripMargin)
      }

      case _ => throw Exception()
  }

  override def getDataFrame(sourceName: String): DataFrame = {
    if partitionKey == null then {
      return getTable(sourceName = sourceName)
    } else {
      return getTablePartition(sourceName = sourceName)
    }
  }

  override def getDataFrameSegmentation(sourceName: String, segmentName: String): DataFrame = {
    throw Exception()
  }

  override def getDataFrameAdHoc(sourceName: String, adHoc: Row): (DataFrame, String) = {
    throw Exception()
  }

  override def getSegments(connection: ConnectionTrait): List[String] = {
    throw Exception()
  }
}