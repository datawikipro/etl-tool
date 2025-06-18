package pro.datawiki.sparkLoader.connection.local.localParquete

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.connection.local.localBase.{LoaderLocalBase, YamlConfig}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, FileStorageTrait, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.io.File

class LoaderLocalParquet(configYaml: YamlConfig) extends LoaderLocalBase(configYaml), ConnectionTrait, DataWarehouseTrait, FileStorageTrait, LazyLogging {
  override def saveRaw(in: String, inLocation: String): Unit = super.saveRaw(in, inLocation)

  override def readDf(location: String, keyPartitions: List[String], valuePartitions: List[String]): DataFrame = {
    readDf(super.getLocation(location = location, keyPartitions = keyPartitions, valuePartitions = valuePartitions))
  }

  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).parquet(s"${configYaml.folder}/${location}")
  }

  override def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit = {
    writeDf(df, super.getLocation(location,partitionName,partitionValue), writeMode)
  }

  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit = throw Exception()

  //  override def getSegments(location: String): List[String] = {
  //    val file = new File(s"${configYaml.folder}/${location}")
  //    val list =  file.listFiles.filter(_.isFile).map(_.getPath).toList
  //    return list
  //  }

  override def readDf(location: String): DataFrame ={
    val loc =location.replace(" ","\\ ")
    val df: DataFrame = SparkObject.spark.read.json(loc)
    return df
  }

  override def close(): Unit = {}

  override def readDfSchema(location: String): DataFrame = throw Exception()

  override def getMasterFolder: String = super.getMasterFolder

  override def deleteFolder(location: String): Boolean = throw Exception()
}

object LoaderLocalParquet extends YamlClass {
  def apply(inConfig: String): LoaderLocalParquet = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    val loader = new LoaderLocalParquet(configYaml)
    return loader
  }
}