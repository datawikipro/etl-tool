package pro.datawiki.sparkLoader

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import pro.datawiki.datawarehouse.DataFrameStream

import scala.concurrent.duration.FiniteDuration
import scala.collection.mutable

object SparkObject extends LazyLogging {
  var localSpark: SparkSession = null
//  var listDataframes: mutable.Map[String, DataFrame] = mutable.Map()
//  def reset():Unit= {
//    listDataframes = mutable.Map()
//  }
  def initSpark(): Unit = {

    val conf = new SparkConf()
//    conf.set("spark.driver.memory", "6g")
//    conf.set("spark.executor.memory", "6g")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("spark.executor.heartbeatInterval", "20000")
    conf.set("spark.network.timeout", "30000")
    conf.set("spark.driver.extraJavaOptions", "java.base/sun.nio.cs=ALL-UNNAMED")
    conf.set("spark.executor.extraJavaOptions", "java.base/sun.nio.cs=ALL-UNNAMED")
    println("--------------------Start session------------------------------------")
    localSpark = SparkSession.builder().
      appName("etl-tool").
      config(conf).
      master("local[*]").
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