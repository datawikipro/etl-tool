package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.connection.{Connection, QueryTrait}
import pro.datawiki.sparkLoader.source.Source

case class YamlConfigSourceKafkaTopic(
                                 server: String,
                                 topic: String
                               ) extends YamlConfigSourceTrait {
  
  override def getDataFrame(sourceName: String): DataFrame = {
    val src = Connection.getConnection(sourceName)
    src match
      case x: QueryTrait => {
        return x.getDataFrameFromTopic("NewTopic")
      }
      case _ => throw Exception()
  }


}