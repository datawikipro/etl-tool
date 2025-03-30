package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.WriteMode
import pro.datawiki.sparkLoader.connection.local.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.connection.minIo.minioBase.LoaderMinIo
import pro.datawiki.sparkLoader.connection.minIo.minioJson.LoaderMinIoJson
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, FileStorageTrait, WriteMode}

import scala.util.Random

class TransformationCacheDatabase(connect: DatabaseTrait) extends TransformationCacheTrait{

  val location: String = connect match
    case x: LoaderMinIo => s"${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
    case x: DatabaseTrait => s"tmp.${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
    case _ => throw Exception()

  def getLocation: String = location
  
  @Override
  def saveTable(in: DataFrameTrait, treadName:String): Unit = {
    in match
      case x: DataFrameOriginal => {
        connect.writeDf(x.get, location, WriteMode.append)
      }
      case _ => throw Exception()
  }

  override def readDirty(): List[DataFrameTrait] = throw Exception()
//
//  def saveTablePartitionAuto(df: DataFrame,
//                             partitionName: List[String]): Unit = {
//    connect.writeDfPartitionAuto(df, location, partitionName,  WriteMode.overwrite)
//  }
//
//  def moveTablePartition(targetSchema: String, targetLocation: String, partitionName: List[String]): Boolean = {
//    connect.moveTablePartition(targetSchema, location, targetSchema, targetLocation, partitionName,  WriteMode.overwrite)
//  }

  @Override
  def readBaseTable(): DataFrame = {
    connect.readDf(location)
  }

  override def saveTable(in: DataFrameTrait): Unit = throw Exception()

  override def append(in: TransformationCacheTrait): Boolean = throw Exception()
}
