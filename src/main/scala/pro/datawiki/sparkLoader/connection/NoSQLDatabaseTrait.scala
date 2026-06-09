package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.exception.UnsupportedOperationException
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

trait NoSQLDatabaseTrait {
  def readDf(location: String): DataFrameTrait
  def writeDf(df: DataFrame, tableFullName: String, writeMode: WriteMode): Unit
}


object NoSQLDatabaseTrait {
  def apply(connection: String, configLocation: String): NoSQLDatabaseTrait = {
    ConnectionTrait.apply(null, connection, configLocation) match {
      case x: NoSQLDatabaseTrait => return x
      case _ => throw UnsupportedOperationException(s"Connection type is not a NoSQLDatabaseTrait: ${connection.getClass.getSimpleName}")
    }
  }
}