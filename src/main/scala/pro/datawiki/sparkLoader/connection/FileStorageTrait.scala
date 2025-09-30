package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.UnsupportedOperationException
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode

trait FileStorageTrait extends ConnectionTrait {
  def saveRaw(in: String, inLocation: String): Unit

  def readDf(location: String): DataFrame

  def readDf(location: String, keyPartitions: List[String], valuePartitions: List[String]): DataFrame

  def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit

  def readDfSchema(location: String): DataFrame
  
  def writeDfPartitionDirect(df: DataFrame,
                             location: String,
                             partitionName: List[String],
                             partitionValue: List[String],
                             writeMode: WriteMode): Unit

  def writeDfPartitionAuto(df: DataFrame,
                           location: String,
                           partitionName: List[String],
                           writeMode: WriteMode): Unit

  def moveTablePartition(oldTableSchema: String, oldTable: String, newTableSchema: String, newTable: String, partitionName: List[String]): Boolean

  def getMasterFolder: String

  def getFolder(location: String): List[String]

  def deleteFolder(location: String): Boolean
}


object FileStorageTrait {
  def apply(connection: String, configLocation: String): FileStorageTrait = {
    ConnectionTrait.apply(null, connection, configLocation) match {
      case storageTrait: FileStorageTrait => return storageTrait
      case _ => throw UnsupportedOperationException(s"Connection type is not a FileStorageTrait: ${connection.getClass.getSimpleName}")
    }
  }
}