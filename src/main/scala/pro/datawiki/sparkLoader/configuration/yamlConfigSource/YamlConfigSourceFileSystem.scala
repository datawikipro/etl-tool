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
      case x: DataWarehouseTrait => {
        var df: DataFrame = x.readDf(s"$tableName")
        df.show()

        df = RunConfig.getPartition match
          case "All" => df
          case _ => df.filter(s"""run_id='${RunConfig.getPartition}'""")
        df.show()

        df = RunConfig.getSubPartition match
          case null => df
          case _ => df.filter(s"""batch_id='${RunConfig.getSubPartition}'""")
        df.show()

        return df
      }
      case _ => throw Exception()
  }

  override def getDataFrameSegmentation(sourceName: String, segmentName: String): DataFrame = {
    val src = Connection.getConnection(sourceName)
    src match
      case x: DataWarehouseTrait =>
        RunConfig.getPartition match
          case "All" => return x.readDf(s"$tableName", segmentName)
          case _ =>  return x.readDf(s"$tableName/${RunConfig.getPartition}/", segmentName)
      case _ => throw Exception()
  }

  override def getDataFrameAdHoc(sourceName: String, adHoc: Row): (DataFrame, String) = {
    throw Exception()
  }
  
  override def getSegments(connection: ConnectionTrait): List[String] = {
    connection match
      case x: FileSystemTrait => 
        return x.getSegments(tableName)
      case _=> throw Exception()
  }
}