package source.s3

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}

class LoaderS3(inConfig:String) {
  var configYaml: YamlConfig = null
  val lines: String = Files.readString(Paths.get(inConfig))

  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)
  configYaml = mapper.readValue(lines, classOf[YamlConfig])

  def modifySpark(spark:SparkSession) :Unit={
    val connectionTimeOut = "600000"
    
    //spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s3endPointLoc)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", getS3AccessKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", getS3SecretKeyAws)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.timeout", connectionTimeOut)
    spark.sparkContext.hadoopConfiguration.set("spark.speculation", "false")
    spark.sparkContext.hadoopConfiguration.set("spark.sql.debug.maxToStringFields", "100")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.attempts.maximum", "1")
    //spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
  }

  def getS3AccessKeyAws:String = configYaml.s3accessKeyAws
  def getS3SecretKeyAws:String = configYaml.s3accessKeyAws
}
