package pro.datawiki.sparkLoader.connection.localJson

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, FileSystemTrait, WriteMode}
import pro.datawiki.sparkLoader.transformation.TransformationCache
import pro.datawiki.sparkLoader.{SparkObject, YamlClass}

import java.io.File

class LoaderLocalJson(configYaml: YamlConfig) extends ConnectionTrait, DataWarehouseTrait, FileSystemTrait {
  
  def saveRaw(in:String,location: String):Unit={
    reflect.io.File(s"${configYaml.folder}/${location.replace(".","/")}").writeAll(in)
  }
  
  override def readDf(location: String): DataFrame = {
    val df: DataFrame = SparkObject.spark.read.json(s"${configYaml.folder}/${location.replace(".","/")}")
    return df
  }

  override def readDf(location: String, segmentName: String): DataFrame = {
    val df: DataFrame = SparkObject.spark.read.json(s"${configYaml.folder}/${location.replace(".","/")}/$segmentName")
    return df
  }

  override def writeDf(location: String, df: DataFrame, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).json(s"${configYaml.folder}/${location.replace(".","/")}")
  }

  override def getSegments(location: String): List[String] = {
    val file = new File(s"${configYaml.folder}/${location.replace(".","/")}")
    val list = file.listFiles.filter(_.isFile).map(_.getName).toList
    return list
  }

  override def writeDf(location: String, df: DataFrame, columnsLogicKey: List[String],columns:List[String], writeMode: WriteMode): Unit = throw Exception()
}

object LoaderLocalJson extends YamlClass {
  def apply(inConfig: String): LoaderLocalJson = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    val loader = new LoaderLocalJson(configYaml)
    return loader
  }
}