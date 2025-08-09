package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.UnsupportedOperationException
import pro.datawiki.sparkLoader.connection.local.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.connection.minIo.minioBase.LoaderMinIo
import pro.datawiki.sparkLoader.connection.minIo.minioBatch.LoaderMinIoBatch
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, FileStorageTrait, WriteMode}

import scala.util.Random

class TransformationCacheNone(connect: DatabaseTrait) extends TransformationCacheTrait {

  @Override
  def saveTable(in: DataFrameTrait, mode:WriteMode): Unit = throw new UnsupportedOperationException("saveTable not implemented in TransformationCacheNone")

  @Override
  def readDirty(): List[DataFrameTrait] = throw new UnsupportedOperationException("readDirty not implemented in TransformationCacheNone")

  @Override
  def readBaseTable(): DataFrame = throw new UnsupportedOperationException("readBaseTable not implemented in TransformationCacheNone")

  @Override
  def append(in: TransformationCacheTrait): Boolean = throw new UnsupportedOperationException("append not implemented in TransformationCacheNone")

  @Override
  def saveTable(in: DataFrameTrait): Unit = throw new UnsupportedOperationException("saveTable not implemented in TransformationCacheNone")

  override def createNewCache: TransformationCacheTrait = throw new UnsupportedOperationException("createNewCache not implemented in TransformationCacheNone")

  override def close(): Unit = throw new UnsupportedOperationException("close not implemented in TransformationCacheNone")

  override def deleteTable(): Unit = throw new UnsupportedOperationException("deleteTable not implemented in TransformationCacheNone")
}
