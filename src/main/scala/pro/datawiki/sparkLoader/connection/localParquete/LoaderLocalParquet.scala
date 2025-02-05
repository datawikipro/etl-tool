package pro.datawiki.sparkLoader.connection.localParquete

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, FileSystemTrait, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject, YamlClass}

import java.io.File
import com.typesafe.scalalogging.LazyLogging

class LoaderLocalParquet(configYaml: YamlConfig) extends ConnectionTrait, DataWarehouseTrait, FileSystemTrait, LazyLogging {

  override def readDf(location: String, segmentName:String): DataFrame = {
    val df: DataFrame = segmentName match
      case null => SparkObject.spark.read.parquet(s"${configYaml.folder}/$location")
      case _ => SparkObject.spark.read.parquet(s"${configYaml.folder}/$location/$segmentName")

    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return df
  }
  
  override def writeDf(location: String, df: DataFrame, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).parquet(s"${configYaml.folder}/${location.replace(".","/")}")
  }

  override def getSegments(location: String): List[String] = {
    val file = new File(s"${configYaml.folder}/${location}")
    val list =  file.listFiles.filter(_.isFile).map(_.getPath).toList
    return list
  }

  override def readDf(location: String): DataFrame = throw Exception()

  override def writeDf(location: String, df: DataFrame, columnsLogicKey: List[String],columns:List[String], writeMode: WriteMode): Unit = throw Exception()
}

object LoaderLocalParquet  extends YamlClass {
  def apply(inConfig: String): LoaderLocalParquet = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    val loader = new LoaderLocalParquet(configYaml)
    return loader
  }
}