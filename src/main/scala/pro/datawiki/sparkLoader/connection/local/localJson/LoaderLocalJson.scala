package pro.datawiki.sparkLoader.connection.local.localJson

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.connection.local.localBase.{LoaderLocalBase, YamlConfig}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, FileStorageTrait, WriteMode}
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.yamlConfiguration.YamlClass

import java.io.File

class LoaderLocalJson(configYaml: YamlConfig) extends LoaderLocalBase(configYaml), DataWarehouseTrait, FileStorageTrait {
  override def saveRaw(in: String, inLocation: String): Unit = super.saveRaw(in, inLocation)

  override def getFolder(location: String): List[String] = super.getFolder(location)

  override def readDf(location: String): DataFrame = {
    val loc = super.getLocation(super.getLocation(location = location))
    val df: DataFrame = SparkObject.spark.read.json(loc)
    return df
  }

  override def readDf(location: String, keyPartitions: List[String], valuePartitions: List[String]): DataFrame = {
    val df: DataFrame = SparkObject.spark.read.json(super.getLocation(location,keyPartitions, valuePartitions))
    return df
  }

  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).json(s"${configYaml.folder}/${location}")
  }

  override def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).json(super.getLocation(location = location, keyPartitions = partitionName, valuePartitions = partitionValue))
  }

  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit = throw Exception()

  override def moveTablePartition(oldTableSchema: String, oldTable: String, newTableSchema: String, newTable: String, partitionName: List[String], writeMode: WriteMode): Boolean = {
    super.moveTablePartition(sourceSchema=oldTableSchema, oldTable=oldTable, newTable=newTable, partitionName= partitionName, writeMode= writeMode)
  }

  override def close(): Unit = {}

  override def readDfSchema(location: String): DataFrame = throw Exception()

  override def getMasterFolder: String = super.getMasterFolder

  override def deleteFolder(location: String): Boolean =throw Exception()
}

object LoaderLocalJson extends YamlClass {
  def apply(inConfig: String): LoaderLocalJson = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    val loader = new LoaderLocalJson(configYaml)
    return loader
  }
}