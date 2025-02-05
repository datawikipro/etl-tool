package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.source.Source

case class YamlConfigSourceKafkaTopic(
                                       topic: String
                                     ) extends YamlConfigSourceTrait {

  override def getDataFrame(sourceName: String): DataFrame = {
    val src = Connection.getConnection(sourceName)
    src match
      case x: QueryTrait => {
        return x.getDataFrameFromTopic(topic)
      }
      case _ => throw Exception()
  }

  override def getDataFrameSegmentation(sourceName: String, segmentName: String): DataFrame = throw Exception()

  override def getDataFrameAdHoc(sourceName: String, adHoc: Row): DataFrame = throw Exception()
  
  override def getSegments(connection: ConnectionTrait): List[String] = throw Exception()
}