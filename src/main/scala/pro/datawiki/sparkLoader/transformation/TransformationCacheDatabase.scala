package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.{ConfigurationException, DataProcessingException, UnsupportedOperationException}
import pro.datawiki.sparkLoader.connection.local.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.connection.minIo.minioBase.LoaderMinIo
import pro.datawiki.sparkLoader.connection.minIo.minioBatch.LoaderMinIoBatch
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, FileStorageTrait, WriteMode}

import scala.util.Random
import scala.collection.mutable

class TransformationCacheDatabase(connect: DatabaseTrait) extends TransformationCacheTrait {

  val location: String = connect match
    case x: LoaderMinIo => s"${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
    case x: DatabaseTrait => s"tmp.${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
    case other => throw new ConfigurationException(s"Неизвестный режим записи: '$other' в TransformationCacheDatabase")

  def getLocation: String = location
  

  override def readDirty(): List[DataFrameTrait] = throw new UnsupportedOperationException("readDirty not implemented in TransformationCacheDatabase")
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
    try {
      return connect.readDf(location)
    } catch {
      case e: Exception => throw new DataProcessingException(s"Failed to read base table: $location", e)
    }
  }

  override def saveTable(in: DataFrameTrait, mode: WriteMode): Unit = {
    mode match {
      case WriteMode.overwrite => connect.writeDf(in.getDataFrame, location, mode)
      case WriteMode.append => connect.writeDf(in.getDataFrame, location, mode)
      case _ => throw new UnsupportedOperationException(s"Unsupported WriteMode: $mode")
    }
  }

  override def append(in: TransformationCacheTrait): Boolean = {
    try {
      val df: DataFrame = in.readBaseTable()
      connect.writeDf(df, location, WriteMode.append)
      return true
    } catch {
      case e: Exception => throw new DataProcessingException("Failed to append cache", e)
    }
  }

  override def createNewCache: TransformationCacheTrait = {
    return new TransformationCacheDatabase(connect)
  }

  override def close(): Unit = throw new UnsupportedOperationException("close not implemented in TransformationCacheDatabase")

  override def deleteTable(): Unit = throw new UnsupportedOperationException("deleteTable not implemented in TransformationCacheDatabase")
}
