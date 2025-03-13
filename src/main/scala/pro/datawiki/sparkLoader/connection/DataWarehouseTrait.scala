package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

trait DataWarehouseTrait {
  def readDf(location: String): DataFrame

  def readDf(location: String, segmentName: String): DataFrame

  def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit

  def writeDf(df: DataFrame, location: String, columnsLogicKey: List[String], columns: List[String], writeMode: WriteMode): Unit

}
