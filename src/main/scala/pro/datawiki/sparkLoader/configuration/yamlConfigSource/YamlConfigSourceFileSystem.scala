package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.configuration.yamlConfigSource.yamlConfigSourceDBTable.YamlConfigSourceDBTableColumn
import pro.datawiki.sparkLoader.configuration.RunConfig
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, DataWarehouseTrait, DatabaseTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

case class YamlConfigSourceFileSystem(
                                       tableName: String,
                                       tableColumns: List[YamlConfigSourceDBTableColumn],
                                       partitionBy: List[String] = List.apply(),
                                       where: String,
                                       limit: Int
                                     ) extends YamlConfigSourceTrait {

  override def getDataFrame(sourceName: String): DataFrameTrait = {
    Connection.getConnection(sourceName) match
      case x: FileStorageTrait => {
        var df = partitionBy.length match
          case 0 => x.readDf(tableName)
          case 1 => x.readDf(tableName, partitionBy, List.apply(RunConfig.getPartition))
          case 2 => x.readDf(tableName, partitionBy, List.apply(RunConfig.getPartition, RunConfig.getSubPartition))
          case _ => throw Exception()

        if where != null then df = df.where(where)
        if limit > 0 then df = df.limit(limit)
        return DataFrameOriginal(df)
      }
      case x: DataWarehouseTrait => {
        var df: DataFrame = x.readDf(s"$tableName")

        df = RunConfig.getPartition match
          case null => df
          case _ => throw Exception()
        df = RunConfig.getSubPartition match
          case null => df
          case _ => throw Exception()

        if limit > 0 then df = df.limit(limit)

        return DataFrameOriginal(df)
      }
      case _ => throw Exception()
  }

  override def getDataFrameSegmentation(sourceName: String, segmentName: String): DataFrame = {
    val src = Connection.getConnection(sourceName)
    src match
      case x: DataWarehouseTrait =>
        RunConfig.getPartition match
          case "All" => return x.readDf(s"$tableName", segmentName)
          case _ => return x.readDf(s"$tableName/${RunConfig.getPartition}/", segmentName)
      case _ => throw Exception()
  }

  override def getDataFrame(sourceName: String, cache: TransformationCacheTrait): DataFrameTrait = {
    return getDataFrame(sourceName= sourceName)
  }

  override def getSegments(connection: ConnectionTrait): List[String] = throw Exception()
}