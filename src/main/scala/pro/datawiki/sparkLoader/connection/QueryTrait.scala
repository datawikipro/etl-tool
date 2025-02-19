package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

trait QueryTrait {
  def getDataFrameBatchFromTopic(topic: String): DataFrame= {throw Exception()}
}
