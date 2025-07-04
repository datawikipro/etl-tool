package pro.datawiki.sparkLoader.task

import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.connection.clickhouse.LoaderClickHouse
import pro.datawiki.sparkLoader.connection.googleAds.LoaderGoogleAds
import pro.datawiki.sparkLoader.connection.jsonApi.LoaderJsonApi
import pro.datawiki.sparkLoader.connection.kafka.kafkaBase.LoaderKafka
import pro.datawiki.sparkLoader.connection.kafka.kafkaMSK.LoaderKafkaMSK
import pro.datawiki.sparkLoader.connection.kafka.kafkaSaslSSL.LoaderKafkaSaslSSL
import pro.datawiki.sparkLoader.connection.local.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.connection.local.localParquete.LoaderLocalParquet
import pro.datawiki.sparkLoader.connection.local.localText.LoaderLocalText
import pro.datawiki.sparkLoader.connection.minIo.{LoaderMinIo, LoaderMinIoStream, minioStream}
import pro.datawiki.sparkLoader.connection.mysql.LoaderMySql
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.connection.s3.LoaderS3
import pro.datawiki.sparkLoader.connection.selenium.LoaderSelenium

import scala.collection.mutable

object Context {
  private var connections: mutable.Map[String, ConnectionTrait] = mutable.Map()
  private var taskTemplate: mutable.Map[String, TaskTemplate] = mutable.Map()


  def closeConnections(): Unit = {
    connections.foreach(i => i._2.close())
  }

  def setConnection(connectionName: String, connectionTrait: ConnectionTrait): Unit = {
    connections += (connectionName, connectionTrait)
  }

  def getConnection(in: String): ConnectionTrait = {
    try {
      return connections(in)
    } catch
      case _ =>
        throw Exception(s"Connection $in not initialize")
  }

  def setTaskTemplate(name: String, task: TaskTemplate): Unit = {
    taskTemplate += (name, task)
  }

  def getTaskTemplate(in: String): TaskTemplate = {
    try {
      return taskTemplate(in)
    } catch
      case _ =>
        throw Exception(s"Task $in not initialize")
  }


}