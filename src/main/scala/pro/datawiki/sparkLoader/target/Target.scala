package pro.datawiki.sparkLoader.target

import pro.datawiki.sparkLoader.configuration.RunConfig
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, DataWarehouseTrait, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

object Target {
  var target: ConnectionTrait = null
  
  def setTarget(in: String): Unit = {
    target = Connection.getConnection(in)
  }
  
  def getTarget: ConnectionTrait = target
}