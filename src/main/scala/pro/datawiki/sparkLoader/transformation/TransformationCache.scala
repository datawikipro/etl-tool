package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader
import pro.datawiki.sparkLoader.connection.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.connection.{Connection, DataWarehouseTrait, WriteMode}

import scala.jdk.CollectionConverters.*
import scala.util.Random

class TransformationCache(connect: DataWarehouseTrait) {
  
  val location:String =s"tmp.${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
  def getLocation:String = location
  
  def saveRaw(rawData:String):Unit={
    connect match
      case x: LoaderLocalJson => x.saveRaw(rawData,location)
      case _ => throw Exception()
  }
  
  def saveTable(in: DataFrame): Unit={
    connect.writeDf(location, in, WriteMode.append)
  }

  def readTable: DataFrame = {
    connect.readDf(location)
  }
}

object TransformationCache {
  var baseConnect: DataWarehouseTrait = null

  def setBaseCache(in: String): Unit = {
    baseConnect = getConnect(in)
  }
  
  private def getConnect(in: String):DataWarehouseTrait = {
    if in == null then {
       throw Exception()
    }

    Connection.getConnection(in) match
      case x: DataWarehouseTrait => return x
      case _ => throw Exception()
  }

  def apply():TransformationCache= {
    new TransformationCache(baseConnect)
  }

//  def apply(in:String): TransformationCache = {
//    new TransformationCache(getConnect(in))
//  }

}