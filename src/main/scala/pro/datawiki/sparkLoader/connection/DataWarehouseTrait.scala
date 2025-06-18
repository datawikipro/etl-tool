package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

trait DataWarehouseTrait extends ConnectionTrait{
  def readDf(location: String): DataFrame

  def readDfSchema(location: String): DataFrame

  def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit

}
