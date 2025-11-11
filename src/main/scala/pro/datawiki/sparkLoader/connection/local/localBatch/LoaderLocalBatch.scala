package pro.datawiki.sparkLoader.connection.local.localBatch

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.connection.fileBased.{FileBaseFormat, FileStorageCommon}
import pro.datawiki.sparkLoader.connection.local.localBase.{LoaderLocalBase, YamlConfig}
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.yamlConfiguration.YamlClass

class LoaderLocalBatch(
  format: FileBaseFormat,
  configYaml: YamlConfig,
  configLocation: String,
  connectionEnum: ConnectionEnum
) extends LoaderLocalBase(configYaml) with ConnectionTrait with FileStorageTrait with LoggingTrait {
  private val _configLocation: String = configLocation

  logInfo(s"Creating Local Batch connection: ${format.toString}")

  override def saveRaw(in: String, inLocation: String): Unit = super.saveRaw(in, inLocation)

  override def readDf(location: String): DataFrame =
    FileStorageCommon
      .createDataFrameReader(format)
      .load(super.getLocation(location = location))

  override def readDf(location: String, keyPartitions: List[String], valuePartitions: List[String], withPartitionOnDataframe:Boolean): DataFrame =
    FileStorageCommon.appendPartitionColumns(
      FileStorageCommon
        .createDataFrameReader(format)
        .load(super.getLocation(location, keyPartitions, valuePartitions)),
      keyPartitions,
      valuePartitions
    )

  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit =
    FileStorageCommon
      .optimizeDataFramePartitions(df)
      .write
      .mode(writeMode.toSparkString)
      .format(format.toString)
      .save(s"${configYaml.folder}/${location}")

  override def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit =
    FileStorageCommon
      .optimizeDataFramePartitions(df)
      .write
      .mode(writeMode.toSparkString)
      .format(format.toString)
      .save(super.getLocation(location, partitionName, partitionValue))

  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit =
    throw pro.datawiki.exception.NotImplementedException("writeDfPartitionAuto not implemented for LoaderLocalBatch")

  override def readDfSchema(location: String): DataFrame =
    throw pro.datawiki.exception.NotImplementedException("readDfSchema not implemented for LoaderLocalBatch")

  override def getFolder(location: String): List[String] = super.getFolder(location)

  override def deleteFolder(location: String): Boolean =
    throw pro.datawiki.exception.NotImplementedException("deleteFolder not implemented for LoaderLocalBatch")

  override def moveTablePartition( oldTable: String, newTable: String, partitionName: List[String]): Boolean =
    super.moveTablePartition(oldTable = oldTable, newTable = newTable, partitionName = partitionName)

  override def getMasterFolder: String = super.getMasterFolder

  override def close(): Unit = {
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = connectionEnum

  override def getConfigLocation(): String = _configLocation
}

object LoaderLocalBatch extends YamlClass {
  def apply(inConfig: String, format: FileBaseFormat, connectionEnum: ConnectionEnum): LoaderLocalBatch = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    new LoaderLocalBatch(format, configYaml, inConfig, connectionEnum)
  }
}


