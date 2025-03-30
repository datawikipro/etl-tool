package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

case class YamlConfigSourceKafkaTopic(
                                       topic: String
                                     ) extends YamlConfigSourceTrait {

  override def getDataFrame(sourceName: String): DataFrameTrait = {
    val src = Connection.getConnection(sourceName)
    src match
      case x: QueryTrait => {
        return DataFrameOriginal(x.getDataFrameBatchFromTopic(topic))
      }
      case _ => throw Exception()
  }

  override def getDataFrameSegmentation(sourceName: String, segmentName: String): DataFrame = throw Exception()

  override def getSegments(connection: ConnectionTrait): List[String] = throw Exception()

  override def getDataFrame(sourceName: String, cache: TransformationCacheTrait): DataFrameTrait = {
    return getDataFrame(sourceName)
  }
}