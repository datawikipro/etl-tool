package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

trait FileStorageTrait extends ConnectionTrait{
  def saveRaw(in: String, inLocation: String): Unit

  def readDf(location: String): DataFrame

  def readDf(location: String, keyPartitions: List[String], valuePartitions: List[String]): DataFrame

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

  def moveTablePartition(oldTableSchema: String, oldTable: String, newTableSchema: String, newTable: String, partitionName: List[String], writeMode: WriteMode): Boolean

  def getMasterFolder: String

  def getFolder(location: String): List[String]
  def deleteFolder(location: String): Boolean
}
