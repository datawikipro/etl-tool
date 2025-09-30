package pro.datawiki.sparkLoader.connection.local.localText

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.connection.local.localBase.{LoaderLocalBase, YamlConfig}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode
import pro.datawiki.yamlConfiguration.YamlClass

class LoaderLocalText(configYaml: YamlConfig) extends LoaderLocalBase(configYaml), ConnectionTrait, FileStorageTrait {
  override def saveRaw(in: String, inLocation: String): Unit = super.saveRaw(in, inLocation)

  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toSparkString).text(s"${configYaml.folder}/${location}")
  }

  override def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit = {
    writeDf(df, s"$location/$partitionName", writeMode)
  }

  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit = throw NotImplementedException("Method not implemented for LocalText")

  override def readDf(location: String, keyPartitions: List[String], valuePartitions: List[String]): DataFrame = {
    readDf(super.getLocation(location = location, keyPartitions = keyPartitions, valuePartitions = keyPartitions))
  }

  override def readDf(location: String): DataFrame = throw NotImplementedException("Method not implemented for LocalText")

  def readFile(location: String): String = {
    val df = SparkObject.spark.read.textFile(s"${configYaml.folder}/$location")
    return ""
  }

  override def close(): Unit = {}

  override def readDfSchema(location: String): DataFrame = throw NotImplementedException("Method not implemented for LocalText")

  override def getMasterFolder: String = throw NotImplementedException("Method not implemented for LocalText")

  override def deleteFolder(location: String): Boolean = throw NotImplementedException("Method not implemented for LocalText")

  override def moveTablePartition(oldTableSchema: String, oldTable: String, newTableSchema: String, newTable: String, partitionName: List[String]): Boolean = throw NotImplementedException("Method not implemented for LocalText")
}

object LoaderLocalText extends YamlClass {
  def apply(inConfig: String): LoaderLocalText = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    val loader = new LoaderLocalText(configYaml)
    return loader
  }
}