package pro.datawiki.sparkLoader.configuration

import org.apache.spark.sql.{Row,DataFrame}
import pro.datawiki.sparkLoader.connection.ConnectionTrait

trait YamlConfigSourceTrait {
  def getDataFrame(sourceName:String): DataFrame
  def getDataFrameSegmentation(sourceName:String, segmentName:String): DataFrame
  def getDataFrameAdHoc(sourceName: String, adHoc: Row): (DataFrame, String)
  def getSegments(connection: ConnectionTrait): List[String]
}
