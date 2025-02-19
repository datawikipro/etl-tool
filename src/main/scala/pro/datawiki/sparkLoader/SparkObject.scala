package pro.datawiki.sparkLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.LazyLogging

object SparkObject extends LazyLogging  {
  val conf = new SparkConf()
  conf.set("spark.driver.memory", "4g")
  conf.set("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
  //conf.set("spark.executor.heartbeatInterval", "20000")
  //conf.set("spark.network.timeout", "30000")
  println("--------------------Start session------------------------------------")
  val spark = SparkSession.builder().
    appName("helloSpark").
    config(conf).
    master("local[1]").
    //master("spark://10.5.0.151:7077").
    getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("--------------------Start end session------------------------------------")
  
  def setHadoopConfiguration(key:String,value:String):Unit={
    spark.sparkContext.hadoopConfiguration.set(key, value)
  }

}