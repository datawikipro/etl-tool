package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.{DataFrameDirty, DataFrameOriginal, DataFrameTrait}
import pro.datawiki.exception.DataProcessingException
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

import scala.util.Random

class TransformationCacheFileStorage() extends TransformationCache {
  var initedTables: List[(FileStorageTrait)] = List.apply()

  val loc: String = s"${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"

  private val locTable = s"$loc/table"
  private val locRaw = s"$loc/raw"

  @Override
  def saveTable(in: DataFrameTrait,
                mode: WriteMode,
                inConnection: ConnectionTrait
               ): Unit = {
    val connect = inConnection match {
      case x: FileStorageTrait => x
      case _ => throw UnsupportedOperationException("Unsupported connection type for TransformationCacheFileStorage")
    }
    in match
      case x: DataFrameOriginal => connect.writeDf(x.getDataFrame, "", locTable, mode)
      case x: DataFrameDirty => connect.writeDf(x.getDataFrame, "", s"${locTable}/${x.getPartitionName}", mode)
      case _ => throw UnsupportedOperationException("Unsupported DataFrame type")
    initedTables = initedTables.appended(connect)
  }

  override def readBaseTable(inConnection: ConnectionTrait): DataFrame = {

    val connect = inConnection match {
      case x: FileStorageTrait => x
      case _ => throw UnsupportedOperationException("Unsupported connection type for TransformationCacheFileStorage")
    }
    try {
      return connect.readDf(locRaw)
    } catch {
      case e: Exception => throw DataProcessingException(s"Failed to read base table: $locRaw", e)
    }
  }

  override def deleteTable(inConnection: ConnectionTrait): Unit = {
    val connect = inConnection match {
      case x: FileStorageTrait => x
      case _ => throw UnsupportedOperationException("Unsupported connection type for TransformationCacheFileStorage")
    }
    connect.deleteFolder(loc)
  }

  override def close(): Unit = {
    initedTables.foreach(deleteTable)
  }
}
