package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader
import pro.datawiki.sparkLoader.connection.*
import pro.datawiki.sparkLoader.connection.WriteMode.overwrite
import pro.datawiki.sparkLoader.connection.local.localJson.LoaderLocalJson
import pro.datawiki.sparkLoader.connection.minIo.minioBase.LoaderMinIo
import pro.datawiki.sparkLoader.connection.minIo.minioJson.LoaderMinIoJson

import scala.jdk.CollectionConverters.*
import scala.util.Random

object TransformationCache {
  var baseConnect: DataWarehouseTrait = null
  val runCacheSchema = s"${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
  private def getConnect(in: String): DataWarehouseTrait = {
    if in == null then {
      throw Exception()
    }

    Connection.getConnection(in) match
      case x: DataWarehouseTrait => return x
      case _ => throw Exception()
  }
  
  def apply(in:String): TransformationCacheTrait = {
    getConnect(in) match
      case x:FileStorageTrait => return TransformationCacheFileStorage(x)
      case _=> throw Exception()
  }
//  
//  def appluByaConnection(connection: Connection):TransformationCacheTrait= {
//    throw Exception()
//  }

}