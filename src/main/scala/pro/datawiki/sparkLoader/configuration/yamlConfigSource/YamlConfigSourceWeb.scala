package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.connection.jsonApi.LoaderJsonApi
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.source.Source
import pro.datawiki.sparkLoader.transformation.{TransformationCache, TransformationCacheTrait}

case class YamlConfigSourceWeb(
                                run: String,
                                isDirty: Boolean,
                                cache: String
                                   ) extends YamlConfigSourceTrait {
  override def getDataFrameAdHoc(sourceName: String, adHoc: Row): DataFrameTrait = {
    val src = Connection.getConnection(sourceName)
    src match
      case x: LoaderSelenium => return x.run(adHoc)
      case x: LoaderJsonApi => return x.run(adHoc,cache)
      case _ => throw Exception()
  }
  
  override def getDataFrame(sourceName: String):  DataFrameTrait = {
    val src = Connection.getConnection(sourceName)
    val a = src match
      case x: LoaderSelenium => x.run(null)
      case x: LoaderJsonApi => x.run(null,cache)
      case _ => throw Exception()
    return a
  }

  override def getSegments(connection: ConnectionTrait): List[String] = throw Exception()

  override def getDataFrameSegmentation(sourceName: String, segmentName: String): DataFrame = throw Exception()

  def getCache(sourceName: String): TransformationCacheTrait = {
    val src = Connection.getConnection(sourceName)
    src match
      case x: LoaderJsonApi => return x.getCache(cache)
      case _ => throw Exception()
  }
}