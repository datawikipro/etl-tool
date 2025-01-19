package pro.datawiki.sparkLoader.connection.localText

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, WriteMode}
import pro.datawiki.sparkLoader.{SparkObject, YamlClass}

class LoaderLocalText(configYaml: YamlConfig) extends ConnectionTrait, DataWarehouseTrait {

  override def readDf(location: String, segmentName:String): DataFrame = {
    val df: DataFrame = segmentName match
      case null => SparkObject.spark.read.text(s"${configYaml.folder}/$location")
      case _ => SparkObject.spark.read.text(s"${configYaml.folder}/$location/$segmentName")

    df.printSchema()
    df.show()
    return df
  }

  override def writeDf(location: String, df: DataFrame, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).text(s"${configYaml.folder}/${location}")
  }

  def readFile(location: String): String = {
    val df = SparkObject.spark.read.textFile(s"${configYaml.folder}/$location")
    df.printSchema()
    df.show()
    return ""
  }
}

object LoaderLocalText  extends YamlClass {
  def apply(inConfig: String): LoaderLocalText = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    val loader = new LoaderLocalText(configYaml)
    return loader
  }
}