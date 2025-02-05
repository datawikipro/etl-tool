package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.configuration.{RunConfig, YamlConfigSourceTrait}
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, DatabaseTrait}

case class YamlConfigSourceDBTable(
                                    tableSchema: String,
                                    tableName: String,
                                    tableColumns: List[YamlConfigSourceDBTableColumn] = List.apply(),
                                    partitionKey: String
                                  ) extends YamlConfigSourceTrait {
  def getColumnNames: List[String] = {
    var lst: List[String] = List.empty
    tableColumns.foreach(i =>
      lst = lst.appended(i.columnName)
    )
    return lst
  }

  private def getTable(sourceName: String): DataFrame = {
    val src = Connection.getConnection(sourceName)
    var df: DataFrame = null
    src match
      case x: DatabaseTrait =>
        getColumnNames.isEmpty match
          case true =>
            return x.getDataFrameBySQL(
              s"""select *
                 |  from ${tableSchema}.${tableName}
                 |  """.stripMargin)
          case false =>
            return x.getDataFrameBySQL(
              s"""select ${getColumnNames.mkString(",")}
                 |  from ${tableSchema}.${tableName}
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

  override def getDataFrameAdHoc(sourceName: String, adHoc: Row): DataFrame = {
    throw Exception()
  }

  override def getSegments(connection: ConnectionTrait): List[String] = {
    throw Exception()
  }
}