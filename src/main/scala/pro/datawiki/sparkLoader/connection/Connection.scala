package pro.datawiki.sparkLoader.connection

import pro.datawiki.sparkLoader.connection.kafka.LoaderKafka
import pro.datawiki.sparkLoader.connection.kafkaMSK.LoaderKafkaMSK
import pro.datawiki.sparkLoader.connection.mysql.LoaderMySql
import pro.datawiki.sparkLoader.connection.postgres.LoaderPostgres
import pro.datawiki.sparkLoader.connection.s3.LoaderS3

import scala.collection.mutable

class Connection {

}

object Connection {
  private var connections: mutable.Map[String, ConnectionTrait] = mutable.Map()
  
  def closeConnections():Unit ={
    connections.foreach(i => i._2.close())
  }
  
  def setConnection(connectionName: String, connectionTrait: ConnectionTrait):Unit = {
    connections += (connectionName,connectionTrait)
  }
  
  def getConnection(in:String):ConnectionTrait = {
    return connections(in)
  }
  
  def initConnection(connection:String, configLocation:String): ConnectionTrait ={
    connection match
      case "mysql" =>  LoaderMySql(configLocation)
      case "postgres" =>  LoaderPostgres(configLocation)
      case "kafka" =>  LoaderKafka(configLocation)
      case "kafkaAmazon" =>  LoaderKafkaMSK(configLocation)
      case "s3Amazon" =>  LoaderS3(configLocation)
      case _ =>
        throw Exception()
  }
}