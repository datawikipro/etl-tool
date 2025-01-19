package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws}
import pro.datawiki.sparkLoader
import pro.datawiki.sparkLoader.connection.{Connection, DataWarehouseTrait, DatabaseTrait, WriteMode}
import pro.datawiki.sparkLoader.transformation.TransformationCache.connect

import scala.jdk.CollectionConverters.*
import scala.util.Random

class TransformationCache {
  val location:String =s"tmp/${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"

  def saveTable(in: DataFrame): Unit={
    connect.writeDf(location, in, WriteMode.append)
  }

  def readTable: DataFrame = {
    connect.readDf(location)
  }
}

object TransformationCache {
  var connect: DataWarehouseTrait = null

  def setCache(in: String): Unit = {
    if in  == null then {
      return 
    }

    Connection.getConnection(in)  match
      case x: DataWarehouseTrait => connect = x
      case _ => throw Exception()
  }


}