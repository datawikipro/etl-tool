package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

trait QueryTrait extends ConnectionTrait {
  def getDataFrameBatchFromTopic(topic: String): DataFrame

  def getListTopics(template: String): DataFrame

  def createTopic(name: String, numPartitions: Int = 1, replicationFactor: Short = 1): Boolean
}
