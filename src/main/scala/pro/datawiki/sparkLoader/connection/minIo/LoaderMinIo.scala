package pro.datawiki.sparkLoader.connection.minIo

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, SparkSession}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, ConnectionTrait, QueryTrait}
import java.nio.file.{Files, Paths}

class LoaderMinIo(configYaml: YamlConfig) extends ConnectionTrait {

  def modifySpark(): Unit = {
    val connectionTimeOut = "600000"
    SparkObject.setHadoopConfiguration("fs.s3a.endpoint", getMinIoHost)
    SparkObject.setHadoopConfiguration("fs.s3a.access.key", getS3AccessKeyAws)
    SparkObject.setHadoopConfiguration("fs.s3a.secret.key", getS3SecretKeyAws)
    SparkObject.setHadoopConfiguration("fs.s3a.establish.timeout", "5000")
    SparkObject.setHadoopConfiguration("fs.s3a.path.style.access", "true")

  }

  def getMinIoHost: String = configYaml.minioHost

  def getS3AccessKeyAws: String = configYaml.s3accessKeyAws

  def getS3SecretKeyAws: String = configYaml.s3secretKeyAws

  override def writeDf(location: String, df: DataFrame): Unit = {
    df.write.mode("overwrite").parquet(s"s3a://tmp/$location")
  }

  def readFile(location: String): String = {
    val df = SparkObject.spark.read.textFile(s"s3a://tmp/$location")
    df.printSchema()
    df.show()
    return ""
  }
}

object LoaderMinIo {
  def apply(inConfig: String): LoaderMinIo = {
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    var configYaml: YamlConfig = mapper.readValue(Files.readString(Paths.get(inConfig)), classOf[YamlConfig])
    val loader = new LoaderMinIo(configYaml)
    loader.modifySpark()
    return loader
  }
}