package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.Row
import pro.datawiki.sparkLoader.SparkObject

class YamlConfigSourceAdHoc(
                             sourceObjectName: String,
                             columnId: List[String],
                             cache: String
                           ) {
  def getAdhocRow: List[Row] = {
    val sql = s"select ${columnId.mkString(",")} from $sourceObjectName"
    return SparkObject.spark.sql(sql).collect().toList
  }
}