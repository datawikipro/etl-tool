package pro.datawiki.sparkLoader.connection.local.localParquete

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.connection.local.localBase.{LoaderLocalBase, YamlConfig}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.io.File

class LoaderLocalParquet(configYaml: YamlConfig) extends LoaderLocalBase(configYaml), ConnectionTrait, FileStorageTrait, LoggingTrait {
  override def saveRaw(in: String, inLocation: String): Unit = super.saveRaw(in, inLocation)

  override def readDf(location: String, keyPartitions: List[String], valuePartitions: List[String]): DataFrame = {
    readDf(super.getLocation(location = location, keyPartitions = keyPartitions, valuePartitions = valuePartitions))
  }

  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toSparkString).parquet(s"${configYaml.folder}/${location}")
  }

  override def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit = {
    writeDf(df, super.getLocation(location, partitionName, partitionValue), writeMode)
  }

  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit = throw NotImplementedException("Method not implemented for LocalParquet")

  //  override def getSegments(location: String): List[String] = {
  //    val file = new File(s"${configYaml.folder}/${location}")
  //    val list =  file.listFiles.filter(_.isFile).map(_.getPath).toList
  //    return list
  //  }

  override def readDf(location: String): DataFrame = {
    val loc = location.replace(" ", "\\ ")
    val df: DataFrame = SparkObject.spark.read.parquet(loc)
    return df
  }

  override def close(): Unit = {}

  override def readDfSchema(location: String): DataFrame = throw NotImplementedException("Method not implemented for LocalParquet")

  override def getMasterFolder: String = super.getMasterFolder

  override def deleteFolder(location: String): Boolean = throw NotImplementedException("Method not implemented for LocalParquet")

  override def moveTablePartition(oldTableSchema: String, oldTable: String, newTableSchema: String, newTable: String, partitionName: List[String]): Boolean = throw NotImplementedException("Method not implemented for LocalParquet")
}

object LoaderLocalParquet extends YamlClass {
  def apply(inConfig: String): LoaderLocalParquet = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    val loader = new LoaderLocalParquet(configYaml)
    return loader
  }
}