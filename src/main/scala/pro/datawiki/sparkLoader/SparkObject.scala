package pro.datawiki.sparkLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.LazyLogging

object SparkObject extends LazyLogging  {
  val conf = new SparkConf()
  //conf.set("spark.driver.memory", "4g")
  //conf.set("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
  println("--------------------Start session------------------------------------")
  val spark = SparkSession.builder().appName("helloSpark").config(conf).master("local[1]").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  println("--------------------Start end session------------------------------------")
  def setHadoopConfiguration(key:String,value:String):Unit={
    spark.sparkContext.hadoopConfiguration.set(key, value)
  }
}