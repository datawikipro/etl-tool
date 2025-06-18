package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

import java.sql.Connection

trait DatabaseTrait extends ConnectionTrait{
  def readDf(location: String): DataFrame

  def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit
  def generateIdMap(inTable:String, domain: String, systemCode:String): Boolean
  def mergeIdMap(inTable:String, domain: String, inSystemCode:String, outSystemCode:String): Boolean
  def truncateTable(tableName:String): Boolean
  def getDataFrameBySQL(sql: String): DataFrame
  def getPartitionsForTable(tableName:String): List[String]
  def getConnection: Connection
  def readDfSchema(in:String) : DataFrame
  def runSQL(in:String): Boolean
}
