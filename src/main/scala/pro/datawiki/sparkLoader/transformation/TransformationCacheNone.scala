package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameOriginal, DataFrameTrait}
import pro.datawiki.sparkLoader.connection.local.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.connection.minIo.minioBase.LoaderMinIo
import pro.datawiki.sparkLoader.connection.minIo.minioJson.LoaderMinIoJson
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, FileStorageTrait, WriteMode}

import scala.util.Random

class TransformationCacheNone(connect: DatabaseTrait) extends TransformationCacheTrait {

  @Override
  def saveTable(in: DataFrameTrait, treadName:String): Unit = throw Exception()

  @Override
  def readDirty(): List[DataFrameTrait] = throw Exception()

  @Override
  def readBaseTable(): DataFrame = throw Exception()

  @Override
  def append(in: TransformationCacheTrait): Boolean = throw Exception()

  @Override
  def saveTable(in: DataFrameTrait): Unit = throw Exception()
}
