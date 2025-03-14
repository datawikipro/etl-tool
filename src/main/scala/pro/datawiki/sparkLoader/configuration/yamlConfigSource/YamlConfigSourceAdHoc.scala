package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.Row
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, QueryTrait}
import pro.datawiki.sparkLoader.source.Source
import pro.datawiki.sparkLoader.transformation.{TransformationCache, TransformationCacheTrait}

class YamlConfigSourceAdHoc(
                             sourceObjectName: String,
                             columnId: List[String],
                             cache: String
                           ) {
  def getAdhocRow: List[Row] = {
    val sql = s"select ${columnId.mkString(",")} from $sourceObjectName"
    return SparkObject.spark.sql(sql).collect().toList
  }
  def getCache: TransformationCacheTrait = {
    TransformationCache.apply(cache)
  }
}