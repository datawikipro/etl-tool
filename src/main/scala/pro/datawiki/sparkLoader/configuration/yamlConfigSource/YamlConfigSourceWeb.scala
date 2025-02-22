package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.connection.jsonApi.LoaderJsonApi
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.source.Source

case class YamlConfigSourceWeb(
                                     run: String
                                   ) extends YamlConfigSourceTrait {
  override def getDataFrameAdHoc(sourceName: String, adHoc: Row): DataFrame = {
    val src = Connection.getConnection(sourceName)
    src match
      case x: LoaderSelenium => return x.run(adHoc)
      case x: LoaderJsonApi => return x.run(adHoc)
      case _ => throw Exception()
  }
  
  override def getDataFrame(sourceName: String): DataFrame = {
    val src = Connection.getConnection(sourceName)
    src match
      case x: LoaderSelenium => return x.run(null)
      case x: LoaderJsonApi => return x.run(null)
      case _ => throw Exception()
  }

  override def getSegments(connection: ConnectionTrait): List[String] = throw Exception()

  override def getDataFrameSegmentation(sourceName: String, segmentName: String): DataFrame = throw Exception()
  
}