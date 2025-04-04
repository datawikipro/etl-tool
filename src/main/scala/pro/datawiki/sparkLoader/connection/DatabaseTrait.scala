package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

import java.sql.Connection

trait DatabaseTrait {
  def readDf(location: String): DataFrame

  def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit

  def getDataFrameBySQL(sql: String): DataFrame

  def insertCCdToIdmap(df: DataFrame, domainName: String, tenantName: String, hasRk: Boolean): Unit

  def getIdmapDataFrame(domainName: String, tenantName: String): DataFrame
  
  def getConnection:Connection
//  def getJdbcConnectString:String
}
