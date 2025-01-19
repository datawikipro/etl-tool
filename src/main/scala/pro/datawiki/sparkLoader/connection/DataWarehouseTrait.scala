package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

trait DataWarehouseTrait {
  def readDf(location: String): DataFrame = throw Exception()
  def readDf(location: String, segmentName:String): DataFrame = throw Exception()
  def writeDf(location: String, df: DataFrame, writeMode: WriteMode): Unit = throw Exception()
  def writeDf(location: String, df: DataFrame, columnsLogicKey:List[String], writeMode: WriteMode): Unit = throw Exception()
  
}
