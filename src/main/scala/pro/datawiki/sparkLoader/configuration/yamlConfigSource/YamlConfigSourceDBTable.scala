package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.{RunConfig, YamlConfigSourceTrait}
import pro.datawiki.sparkLoader.connection.{Connection, DatabaseTrait}

case class YamlConfigSourceDBTable(
                                    tableSchema: String,
                                    tableName: String,
                                    tableColumns: List[YamlConfigSourceDBTableColumn],
                                    partitionKey: String
                                  ) extends YamlConfigSourceTrait{
  def getColumnNames: List[String] = {
    var lst :List[String] = List.empty
    tableColumns.foreach(i=>
      //lst = lst.appended(f"cast(${i.columnName} as String) as ${i.columnName}")
      lst = lst.appended(i.columnName)
    )
    return lst
  }

  private def getTable(sourceName: String                      ): DataFrame = {
    val src = Connection.getConnection(sourceName)

    var df: DataFrame = null
    src match
      case x: DatabaseTrait =>
        return x.getDataFrameBySQL(
          s"""select ${getColumnNames.mkString(",")}
             |  from ${tableSchema}.${tableName}
             |  limit 100
             |  """.stripMargin)
      case _ => throw Exception()
  }

  private def getTablePartition(sourceName: String                              ): DataFrame = {
    val src = Connection.getConnection(sourceName)

    var df: DataFrame = null
    src match
      case x: DatabaseTrait =>
        return x.getDataFrameBySQL(
          s"""select ${getColumnNames.mkString(",")}
             |  from ${tableSchema}.${tableName}
             | where $partitionKey = ${RunConfig.getPartition}""".stripMargin)
      case _ => throw Exception()
  }

  override def getDataFrame(sourceName:String): DataFrame = {
    if partitionKey == null then {
      return getTable(sourceName = sourceName)
    } else {
      return getTablePartition(sourceName = sourceName)
    }
  }

}