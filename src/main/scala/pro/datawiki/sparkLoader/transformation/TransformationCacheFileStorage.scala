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

  val location: String = connect match
    case x: LoaderMinIo => s"${TransformationCache.runCacheSchema}/${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
    case _ => throw Exception()

  def getLocation: String = location

  @Override
  def saveRaw(rawData: String): Unit = {
    connect match
      case x: LoaderLocalJson => x.saveRaw(rawData, location)
      case x: LoaderMinIoJson => x.saveRaw(rawData, location)
      case _ => throw Exception()
  }

  def saveTable(in: DataFrameTrait): Unit = {
    in match
      case x: DataFrameOriginal => connect.writeDf(x.get, location, WriteMode.append)
      case x: DataFrameDirty => connect.writeDf(x.get, s"${location}/${x.getPartitionName}", WriteMode.append)
      case _ => throw Exception()
  }

  def saveTablePartitionAuto(df: DataFrame,
                             partitionName: List[String]): Unit = {
    connect.writeDfPartitionAuto(df, location, partitionName, overwrite)
  }

  def moveTablePartition(targetSchema: String, targetLocation: String, partitionName: List[String]): Boolean = {
    connect.moveTablePartition(targetSchema, location, targetSchema, targetLocation, partitionName, overwrite)
  }

  @Override
  def readTable: DataFrame = {
    connect.readDf(location)
  }

  override def readDirty: List[DataFrameTrait] = {
    var list: List[DataFrameTrait] = List.apply()
    connect.getFolder(location).foreach(i => {
      list = list.appended(DataFrameDirty(i, connect.readDf(s"${location}/$i")))
    })
    return list
  }
}
