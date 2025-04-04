package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

case class YamlConfigSourceDBSQL(
                                  sql: String,
                                ) extends YamlConfigSourceTrait {
  override def getDataFrame(sourceName: String): DataFrameTrait = {
    val src = Connection.getConnection(sourceName)
    var df: DataFrame = null
    src match
      case x: DatabaseTrait =>
        return  DataFrameOriginal( x.getDataFrameBySQL(sql))
      case _ => throw Exception()
  }

  override def getDataFrame(sourceName: String, cache: TransformationCacheTrait): DataFrameTrait = throw Exception()
  override def getDataFrameSegmentation(sourceName: String, segmentName: String): DataFrame = throw Exception()

  override def getSegments(connection: ConnectionTrait): List[String] = throw Exception()

}