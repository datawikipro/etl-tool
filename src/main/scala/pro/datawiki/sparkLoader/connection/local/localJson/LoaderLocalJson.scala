package pro.datawiki.sparkLoader.connection.local.localJson

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.connection.local.localBase.{LoaderLocalBase, YamlConfig}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, FileStorageTrait, WriteMode}
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.sparkLoader.{SparkObject, YamlClass}

import java.io.File

class LoaderLocalJson(configYaml: YamlConfig) extends LoaderLocalBase(configYaml), ConnectionTrait, DataWarehouseTrait, FileStorageTrait {
  override def saveRaw(in: String, inLocation: String): Unit = super.saveRaw(in,inLocation)
  override def getFolder(location: String): List[String] = super.getFolder(location)

  override def readDf(location: String): DataFrame = {
    val df: DataFrame = SparkObject.spark.read.json(s"${configYaml.folder}/${location.replace(".", "/")}")
    return df
  }

  override def readDf(location: String, keyPartitions: List[String], valuePartitions: List[String]): DataFrame = {
    var postfix: String = ""
    keyPartitions.zipWithIndex.foreach { case (value, index) =>
      postfix = s"$postfix/${keyPartitions(index)}=${valuePartitions(index)}"
    }
    val df: DataFrame = SparkObject.spark.read.json(s"${configYaml.folder}/${postfix}")
    return df
  }

  override def readDf(location: String, segmentName: String): DataFrame = {
    val df: DataFrame = SparkObject.spark.read.json(s"${configYaml.folder}/${location.replace(".", "/")}/$segmentName")
    return df
  }

  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).json(s"${configYaml.folder}/${location.replace(".", "/")}")
  }

  override def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).json(super.getLocation(location = location, keyPartitions = partitionName, valuePartitions = partitionValue))
  }

  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit = throw Exception()

  //  override def getSegments(location: String): List[String] = {
  //    val file = new File(s"${configYaml.folder}/${location.replace(".","/")}")
  //    val list = file.listFiles.filter(_.isFile).map(_.getName).toList
  //    return list
  //  }

  override def moveTablePartition(oldTableSchema: String, oldTable: String, newTableSchema: String, newTable: String, partitionName: List[String], writeMode: WriteMode): Boolean = throw Exception()

  override def writeDf(df: DataFrame, location: String, columnsLogicKey: List[String], columns: List[String], writeMode: WriteMode): Unit = throw Exception()
}

object LoaderLocalJson extends YamlClass {
  def apply(inConfig: String): LoaderLocalJson = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    val loader = new LoaderLocalJson(configYaml)
    return loader
  }
}