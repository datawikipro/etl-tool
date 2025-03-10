package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

trait DataWarehouseTrait {
  def readDf(location: String): DataFrame

  def readDf(location: String, segmentName: String): DataFrame

  def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit

  def writeDfPartitionDirect(df: DataFrame,
                             location: String,
                             partitionName: List[String],
                             partitionValue: List[String],
                             writeMode: WriteMode): Unit

  def writeDfPartitionAuto(df: DataFrame,
                           location: String,
                           partitionName: List[String],
                           writeMode: WriteMode): Unit

  def moveTablePartition(oldTableSchema: String, oldTable: String, newTableSchema: String, newTable: String, partitionName: List[String], writeMode: WriteMode): Boolean = throw Exception()

  def writeDf(df: DataFrame, location: String, columnsLogicKey: List[String], columns: List[String], writeMode: WriteMode): Unit

}
