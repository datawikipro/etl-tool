package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader
import pro.datawiki.sparkLoader.connection.WriteMode.overwrite
import pro.datawiki.sparkLoader.connection.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.connection.minIo.minioBase.LoaderMinIo
import pro.datawiki.sparkLoader.connection.minIo.minioJson.LoaderMinIoJson
import pro.datawiki.sparkLoader.connection.{Connection, DataWarehouseTrait, DatabaseTrait, WriteMode}

import scala.jdk.CollectionConverters.*
import scala.util.Random

class TransformationCache(connect: DataWarehouseTrait) {

  val location: String = connect match
    case x: LoaderMinIo => s"${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
    case x: DatabaseTrait => s"tmp.${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
    case _ => throw Exception()

  def getLocation: String = location

  def saveRaw(rawData: String): Unit = {
    connect match
      case x: LoaderLocalJson => x.saveRaw(rawData, location)
      case x: LoaderMinIoJson => x.saveRaw(rawData, location)
      case _ => throw Exception()
  }

  def saveTable(in: DataFrame): Unit = {
    connect.writeDf(in, location, WriteMode.append)
  }

  def saveTablePartitionAuto(df: DataFrame,
                             partitionName: List[String]): Unit = {
    connect.writeDfPartitionAuto(df, location, partitionName, overwrite)
  }

  def moveTablePartition(targetSchema: String, targetLocation: String, partitionName: List[String]): Boolean = {
    connect.moveTablePartition(targetSchema, location, targetSchema, targetLocation, partitionName, overwrite)
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

  private def getConnect(in: String): DataWarehouseTrait = {
    if in == null then {
      throw Exception()
    }

    Connection.getConnection(in) match
      case x: DataWarehouseTrait => return x
      case _ => throw Exception()
  }

  def apply(): TransformationCache = {
    if baseConnect == null then throw Exception()
    new TransformationCache(baseConnect)
  }

  //  def apply(in:String): TransformationCache = {
  //    new TransformationCache(getConnect(in))
  //  }

}