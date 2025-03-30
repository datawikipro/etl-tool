package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameDirty, DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.WriteMode.overwrite
import pro.datawiki.sparkLoader.connection.local.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, FileStorageTrait, WriteMode}
import pro.datawiki.sparkLoader.connection.minIo.minioBase.LoaderMinIo
import pro.datawiki.sparkLoader.connection.minIo.minioJson.LoaderMinIoJson

import scala.util.Random

class TransformationCacheFileStorage(connect: FileStorageTrait) extends TransformationCacheTrait {

  def getConnect:FileStorageTrait =connect

  def checkConnect(in : FileStorageTrait):Boolean = {
    if connect == in then return true
    return false
  }

  val loc: String = connect match
    case x: LoaderMinIo => s"${TransformationCache.runCacheSchema}/${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
    case _ => throw Exception()

  val locTable = s"$loc/table"
  val locRaw = s"$loc/raw"

  @Override
  def saveRaw(rawData: String): Unit = {
    connect match
      case x: FileStorageTrait => x.saveRaw(rawData, locRaw)
      case _ => throw Exception()
  }

  @Override
  def saveTable(in: DataFrameTrait): Unit = {
    in match
      case x: DataFrameOriginal => connect.writeDf(x.get, locTable, WriteMode.append)
      case x: DataFrameDirty => connect.writeDf(x.get, s"${locTable}/${x.getPartitionName}", WriteMode.append)
      case _ => throw Exception()
  }

  def saveTablePartitionAuto(df: DataFrame,
                             partitionName: List[String]): Unit = {
    connect.writeDfPartitionAuto(df, locTable, partitionName, overwrite)
  }

  def moveTablePartition(targetSchema: String, targetLocation: String, partitionName: List[String]): Boolean = {
    connect.moveTablePartition(targetSchema, locTable, targetSchema, targetLocation, partitionName, overwrite)
  }

  @Override
  def readBaseTable(): DataFrame = {
    connect.readDf(locRaw)
  }

  override def readDirty(): List[DataFrameTrait] = {
    var list: List[DataFrameTrait] = List.apply()
    connect.getFolder(s"$locTable").foreach(i => {
      list = list.appended(DataFrameDirty(i, connect.readDf(s"${locTable}/$i")))
    })
    return list
  }

  override def append(in: TransformationCacheTrait): Boolean = {
    in match
      case x:TransformationCacheFileStorage => {
        if !x.checkConnect(connect) then throw Exception()
        connect.moveTablePartition(x.getConnect.getMasterFolder,
          x.locTable,
          connect.getMasterFolder,
          locTable,
          List.apply(),
          WriteMode.append)
      }
      case _ => throw Exception()
  }

}
