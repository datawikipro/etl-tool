package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.UnsupportedOperationException
import pro.datawiki.sparkLoader.connection.local.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.connection.minIo.minioBase.LoaderMinIo
import pro.datawiki.sparkLoader.connection.minIo.minioBatch.LoaderMinIoBatch
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

import scala.util.Random

class TransformationCacheNone() extends TransformationCache {

  @Override
  def saveTable(in: DataFrameTrait, mode: WriteMode, connection: ConnectionTrait): Unit = throw UnsupportedOperationException("saveTable not implemented in TransformationCacheNone")

  @Override
  def readDirty(connection: ConnectionTrait): List[DataFrameTrait] = throw UnsupportedOperationException("readDirty not implemented in TransformationCacheNone")

  @Override
  def readBaseTable(connect: ConnectionTrait): DataFrame = throw UnsupportedOperationException("readBaseTable not implemented in TransformationCacheNone")

  @Override
  def saveTable(in: DataFrameTrait): Unit = throw UnsupportedOperationException("saveTable not implemented in TransformationCacheNone")

  override def close(): Unit = throw UnsupportedOperationException("close not implemented in TransformationCacheNone")

  override def deleteTable(connection: ConnectionTrait): Unit = throw UnsupportedOperationException("deleteTable not implemented in TransformationCacheNone")
}