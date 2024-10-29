package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

trait QueryTrait {
  def getDataFrameFromTopic(topic: String): DataFrame= {throw Exception()}
}
