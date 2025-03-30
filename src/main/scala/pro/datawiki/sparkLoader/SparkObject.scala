package pro.datawiki.sparkLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration.FiniteDuration

object SparkObject extends LazyLogging {
  var localSpark: SparkSession = null

  def initSpark(): Unit = {

    val conf = new SparkConf()
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("spark.executor.heartbeatInterval", "20000")
    conf.set("spark.network.timeout", "30000")
    println("--------------------Start session------------------------------------")
    localSpark = SparkSession.builder().
      appName("etl-tool").
      config(conf).
      master("local[1]").
      //      master("spark://192.168.31.81:7077").
      getOrCreate()
    localSpark.sparkContext.setLogLevel("ERROR")

    println("--------------------Start end session------------------------------------")
  }

  def spark: SparkSession = {
    if localSpark == null then initSpark()

    return localSpark
  }

  def setHadoopConfiguration(key: String, value: String): Unit = {
    spark.sparkContext.hadoopConfiguration.set(key, value)
  }

}