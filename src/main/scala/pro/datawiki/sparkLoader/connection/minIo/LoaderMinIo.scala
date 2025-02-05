package pro.datawiki.sparkLoader.connection.minIo

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, FileSystemTrait, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject, YamlClass}
import com.typesafe.scalalogging.LazyLogging

class LoaderMinIo(configYaml: YamlConfig) extends ConnectionTrait, DataWarehouseTrait, FileSystemTrait, LazyLogging {

  def modifySpark(): Unit = {
    val connectionTimeOut = "600000"
    SparkObject.setHadoopConfiguration("fs.s3a.endpoint", getMinIoHost)
    SparkObject.setHadoopConfiguration("fs.s3a.access.key", getAccessKey)
    SparkObject.setHadoopConfiguration("fs.s3a.secret.key", getSecretKey)
    SparkObject.setHadoopConfiguration("fs.s3a.establish.timeout", "5000")
    SparkObject.setHadoopConfiguration("fs.s3a.path.style.access", "true")
  }

  private def getMinIoHost: String = configYaml.minioHost

  private def getAccessKey: String = configYaml.accessKey

  private def getSecretKey: String = configYaml.secretKey
  
  override def readDf(location: String, segmentName:String): DataFrame = {
    val df: DataFrame = SparkObject.
      spark.
      read.
      parquet(s"s3a://${configYaml.bucket}/$location/$segmentName/")

    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return df
  }

  override def readDf(location: String): DataFrame = {
    val df: DataFrame = SparkObject.
      spark.
      read.
      parquet(s"s3a://${configYaml.bucket}/$location/")

    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return df
  }
  override def writeDf(location: String, df: DataFrame, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).parquet(s"s3a://${configYaml.bucket}/${location.replace(".","/")}/")
  }

  override def writeDf(location: String, df: DataFrame, columnsLogicKey: List[String],columns:List[String], writeMode: WriteMode): Unit = throw Exception()
  override def getSegments(location: String): List[String] = {
    throw Exception()
//    val file = new File(s"s3a://${configYaml.bucket}/$location/")
//    val list = file.listFiles.filter(_.isFile).map(_.getPath).toList
//    return list
  }
}

object LoaderMinIo  extends YamlClass {
  def apply(inConfig: String): LoaderMinIo = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    val loader = new LoaderMinIo(configYaml)
    loader.modifySpark()
    return loader
  }
}