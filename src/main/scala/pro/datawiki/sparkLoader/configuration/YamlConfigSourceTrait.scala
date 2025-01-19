package pro.datawiki.sparkLoader.configuration

import org.apache.spark.sql.{Row,DataFrame}
import pro.datawiki.sparkLoader.connection.ConnectionTrait

trait YamlConfigSourceTrait {
  def getDataFrame(sourceName:String): DataFrame  = throw Exception()
  def getDataFrameSegmentation(sourceName:String, segmentName:String): DataFrame  = throw Exception()
  def getDataFrameAdHoc(sourceName:String, adHoc: Row): DataFrame = throw Exception()
  def getSegments(connection: ConnectionTrait): List[String]
}
