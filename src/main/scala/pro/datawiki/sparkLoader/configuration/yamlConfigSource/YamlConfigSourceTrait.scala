package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

trait YamlConfigSourceTrait {
  def getDataFrame(sourceName:String, cache: TransformationCacheTrait): DataFrameTrait
  def getDataFrame(sourceName:String): DataFrameTrait
  def getDataFrameSegmentation(sourceName:String, segmentName:String): DataFrame

  def getSegments(connection: ConnectionTrait): List[String]
}
