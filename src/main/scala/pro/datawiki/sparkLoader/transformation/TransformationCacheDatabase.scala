package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.{DataProcessingException, UnsupportedOperationException}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.{SCDType, WriteMode}

import scala.util.Random

class TransformationCacheDatabase() extends TransformationCache {
  val tmpSchema = "tmp"
  val location: String = s"${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"

  def getLocation: String = s"$tmpSchema.$location"

  @Override
  def readBaseTable(inConnection: ConnectionTrait): DataFrame = {
    val connect: DatabaseTrait = inConnection match {
      case x: DatabaseTrait => x
      case _ => throw UnsupportedOperationException(s"Unsupported connection type for TransformationCacheDatabase: ${inConnection.getClass.getSimpleName}")
    }

    try {
      return connect.readDf(tmpSchema,location)
    } catch {
      case e: Exception => throw DataProcessingException(s"Failed to read base table: $location", e)
    }
  }
  
  override def saveTable(in: DataFrameTrait, mode: WriteMode, inConnection: ConnectionTrait): Unit = {
    val connect: DatabaseTrait = inConnection match {
      case x: DatabaseTrait => x
      case _ => throw UnsupportedOperationException(s"Unsupported connection type for TransformationCacheDatabase: ${inConnection.getClass.getSimpleName}")
    }
    mode match {
      case WriteMode.overwriteTable => connect.writeDf(in.getDataFrame, tmpSchema,location, mode, SCDType.SCD_0, List.empty)
      case WriteMode.append => connect.writeDf(in.getDataFrame, tmpSchema,location, mode, SCDType.SCD_0, List.empty)
      case _ => throw UnsupportedOperationException(s"Unsupported WriteMode: $mode")
    }
  }

  override def close(): Unit = throw UnsupportedOperationException("close not implemented in TransformationCacheDatabase")

  override def deleteTable(connection: ConnectionTrait): Unit = throw UnsupportedOperationException("deleteTable not implemented in TransformationCacheDatabase")

}