package pro.datawiki.sparkLoader.connection.s3

import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.dictionaryEnum.ConnectionEnum
import pro.datawiki.yamlConfiguration.YamlClass

class LoaderS3(configYaml: YamlConfig, configLocation: String) extends ConnectionTrait {
  private val _configLocation: String = configLocation
  
  logInfo("Creating Amazon S3 connection")

  def modifySpark(): Unit = {
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

  def getS3AccessKeyAws: String = configYaml.s3accessKeyAws

  def getS3SecretKeyAws: String = configYaml.s3secretKeyAws

  override def close(): Unit = {
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = {
    ConnectionEnum.s3Amazon
  }

  override def getConfigLocation(): String = {
    _configLocation
  }
}

object LoaderS3 extends YamlClass {
  def apply(inConfig: String): LoaderS3 = {
    val loader = new LoaderS3(mapper.readValue(getLines(inConfig), classOf[YamlConfig]), inConfig)
    loader.modifySpark()
    return loader
  }
}