package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameDirty, DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.WriteMode.overwrite
import pro.datawiki.sparkLoader.connection.local.localBase.LoaderLocalBase
import pro.datawiki.sparkLoader.connection.minIo.minioBase.LoaderMinIo
import pro.datawiki.sparkLoader.connection.{FileStorageTrait, WriteMode}

import scala.util.Random

class TransformationCacheFileStorage(connect: FileStorageTrait) extends TransformationCacheTrait {
  override def createNewCache: TransformationCacheTrait = TransformationCacheFileStorage(connect)

  def getConnect: FileStorageTrait = connect

  private def checkConnect(in: FileStorageTrait): Boolean = {
    if connect == in then return true
    return false
  }

  val loc: String = connect match
    case x: LoaderMinIo => s"${TransformationCache.runCacheSchema}/${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
    case x: LoaderLocalBase => s"${TransformationCache.runCacheSchema}/${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"    
    case _ => throw Exception()

  private val locTable = s"$loc/table"
  private val locRaw = s"$loc/raw"

  @Override
  def saveRaw(rawData: String): Unit = connect.saveRaw(rawData, locRaw)

  @Override
  def saveTable(in: DataFrameTrait, mode:WriteMode): Unit = {
    in match
      case x: DataFrameOriginal => connect.writeDf(x.getDataFrame, locTable, mode)
      case x: DataFrameDirty => connect.writeDf(x.getDataFrame, s"${locTable}/${x.getPartitionName}", mode)
      case _ => throw Exception()
  }

  def saveTablePartitionAuto(df: DataFrame, partitionName: List[String]): Unit = {
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
      case x: TransformationCacheFileStorage => {
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

  override def deleteTable(): Unit = {
    connect.deleteFolder(loc)
  }
  
  override def close(): Unit = {
    deleteTable()
  }
}
