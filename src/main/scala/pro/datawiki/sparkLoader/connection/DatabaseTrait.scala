package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataColumn, TableMetadataType}

import java.sql.Connection

trait DatabaseTrait extends ConnectionTrait{
  def readDf(location: String): DataFrame
  def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit
  def truncateTable(tableName:String): Boolean
  def getDataFrameBySQL(sql: String): DataFrame
  def getPartitionsForTable(tableName:String): List[String]
  def getConnection: Connection
  def readDfSchema(in:String) : DataFrame
  def runSQL(in:String): Boolean
  def getTableMetadata(tableSchema:String, tableName:String): TableMetadata

}

object DatabaseTrait {
  def apply(connection: String, configLocation: String): DatabaseTrait = {
    ConnectionTrait.apply(connection, configLocation) match {
      case x: DatabaseTrait => return x
      case _=> throw Exception()
    }
  }
}