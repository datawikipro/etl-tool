package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.configuration.{RunConfig, YamlConfigSourceTrait}
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, DataWarehouseTrait, DatabaseTrait, FileSystemTrait}

case class YamlConfigSourceFileSystem(
                                    tableName: String,
                                    tableColumns: List[YamlConfigSourceDBTableColumn],
                                    partitionKey: String
                                  ) extends YamlConfigSourceTrait{
  override def getDataFrameSegmentation(sourceName: String, segmentName: String): DataFrame = {
    val src = Connection.getConnection(sourceName)
    src match
      case x: DataWarehouseTrait =>
        return x.readDf(tableName, segmentName)
      case _ => throw Exception()
  }

  override def getSegments(connection: ConnectionTrait): List[String] = {
    connection match
      case x: FileSystemTrait => 
        return x.getSegments(tableName)
      case _=> throw Exception()
  }
}