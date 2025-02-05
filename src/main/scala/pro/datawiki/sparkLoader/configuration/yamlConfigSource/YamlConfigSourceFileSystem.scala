package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.configuration.{RunConfig, YamlConfigSourceTrait}
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, DataWarehouseTrait, DatabaseTrait, FileSystemTrait}

case class YamlConfigSourceFileSystem(
                                    tableName: String,
                                    tableColumns: List[YamlConfigSourceDBTableColumn],
                                    partitionKey: String
                                  ) extends YamlConfigSourceTrait{

  override def getDataFrame(sourceName: String): DataFrame = {
    val src = Connection.getConnection(sourceName)
    src match
      case x: DataWarehouseTrait =>
        return x.readDf(tableName)
      case _ => throw Exception()
  }

  override def getDataFrameSegmentation(sourceName: String, segmentName: String): DataFrame = {
    val src = Connection.getConnection(sourceName)
    src match
      case x: DataWarehouseTrait =>
        return x.readDf(tableName, segmentName)
      case _ => throw Exception()
  }

  override def getDataFrameAdHoc(sourceName: String, adHoc: Row): DataFrame = {
    throw Exception()
  }
  
  override def getSegments(connection: ConnectionTrait): List[String] = {
    connection match
      case x: FileSystemTrait => 
        return x.getSegments(tableName)
      case _=> throw Exception()
  }
}