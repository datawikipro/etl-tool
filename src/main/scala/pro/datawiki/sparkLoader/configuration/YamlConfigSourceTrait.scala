package pro.datawiki.sparkLoader.configuration

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.connection.ConnectionTrait

trait YamlConfigSourceTrait {
  def getDataFrame(sourceName:String): DataFrameTrait
  def getDataFrameSegmentation(sourceName:String, segmentName:String): DataFrame
  def getDataFrameAdHoc(sourceName: String, adHoc: Row): (DataFrameTrait)
  def getSegments(connection: ConnectionTrait): List[String]
}
