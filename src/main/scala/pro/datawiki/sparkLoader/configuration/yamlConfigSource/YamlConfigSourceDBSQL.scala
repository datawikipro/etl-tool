package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.{DataFrame, Row}
import pro.datawiki.sparkLoader.configuration.YamlConfigSourceTrait
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, DatabaseTrait}

case class YamlConfigSourceDBSQL(
                                    sql: String,
                                  ) extends YamlConfigSourceTrait{
  override def getDataFrame(sourceName:String): DataFrame = {
    val src = Connection.getConnection(sourceName)
    var df: DataFrame = null
    src match
      case x: DatabaseTrait =>
        return x.getDataFrameBySQL(sql)
      case _ => throw Exception()

  }

  override def getDataFrameSegmentation(sourceName: String, segmentName: String): DataFrame = throw Exception()

  override def getDataFrameAdHoc(sourceName: String, adHoc: Row): DataFrame = throw Exception()

  override def getSegments(connection: ConnectionTrait): List[String] = throw Exception()
}