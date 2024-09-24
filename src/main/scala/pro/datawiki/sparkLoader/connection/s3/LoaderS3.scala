package pro.datawiki.sparkLoader.connection.s3

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, SparkSession}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import java.nio.file.{Files, Paths}

class LoaderS3(configYaml: YamlConfig) extends ConnectionTrait {
  
  def modifySpark() :Unit={
    val connectionTimeOut = "600000"
    
    SparkObject.setHadoopConfiguration("fs.s3a.access.key", getS3AccessKeyAws)
    SparkObject.setHadoopConfiguration("fs.s3a.secret.key", getS3SecretKeyAws)
    SparkObject.setHadoopConfiguration("fs.s3a.connection.timeout", connectionTimeOut)
    SparkObject.setHadoopConfiguration("spark.speculation", "false")
    SparkObject.setHadoopConfiguration("spark.sql.debug.maxToStringFields", "100")
    SparkObject.setHadoopConfiguration("fs.s3a.path.style.access", "true")
    SparkObject.setHadoopConfiguration("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    SparkObject.setHadoopConfiguration("fs.s3a.connection.ssl.enabled", "false")
    SparkObject.setHadoopConfiguration("fs.s3a.attempts.maximum", "1")
    //spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
  }

  def getS3AccessKeyAws:String = configYaml.s3accessKeyAws
  def getS3SecretKeyAws:String = configYaml.s3secretKeyAws

  override def writeDf(location:String,df:DataFrame): Unit={
    df.write.mode("overwrite").parquet(s"s3a://bigdata-stg/$location")
  }

}

object LoaderS3{
  def apply (inConfig:String):LoaderS3 = {
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    var configYaml: YamlConfig = mapper.readValue(Files.readString(Paths.get(inConfig)), classOf[YamlConfig])
    val loader = new LoaderS3(configYaml)
    loader.modifySpark()
    return loader
  }
}