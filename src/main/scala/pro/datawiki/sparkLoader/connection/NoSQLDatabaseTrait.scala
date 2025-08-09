package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

trait NoSQLDatabaseTrait {
  def readDf(location: String): DataFrame
}


object NoSQLDatabaseTrait {
  def apply(connection: String, configLocation: String): NoSQLDatabaseTrait = {
    ConnectionTrait.apply(connection, configLocation) match {
      case x: NoSQLDatabaseTrait => return x
      case _=> throw Exception()
    }
  }
}