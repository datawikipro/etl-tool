package pro.datawiki.sparkLoader.target

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.{Connection, ConnectionTrait, DataWarehouseTrait, WriteMode}

object Target {
  var target: ConnectionTrait = null
  
  def setTarget(in: String): Unit = {
    target = Connection.getConnection(in)
  }

  def writeTarget(targetFile:String, columnsLogicKey:List[String]): Unit = {
    val df = SparkObject.spark.sql("select * from target")
    target match
      case x: DataWarehouseTrait => x.writeDf(targetFile, df, columnsLogicKey, WriteMode.append)
      case _ => throw Exception()
  }

  def writeTarget(targetFile: String): Unit = {
    val df = SparkObject.spark.sql("select * from target")
    target match
      case x: DataWarehouseTrait => x.writeDf(targetFile, df, WriteMode.append)
      case _ => throw Exception()
  }
  
}