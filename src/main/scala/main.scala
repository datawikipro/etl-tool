import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import source.mysql.LoaderMySql
import source.s3.LoaderS3
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.nio.file.{Files, Paths}


@main
def sparkRun(): Unit = {
  val loadS3:LoaderS3 = LoaderS3("config/s3.yaml")
  val loadMysql:LoaderMySql = LoaderMySql("config/mysql.yaml")

//  val conf = new SparkConf()
//  conf.set("spark.driver.memory", "4g")
//  val spark = SparkSession.builder().appName("helloSpark").config(conf).master("local[*]").getOrCreate()

//  loadS3.modifySpark(spark)

  var configYaml: YamlListTables = null
  val lines: String = Files.readString(Paths.get("config/list.yaml"))

  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)
  configYaml = mapper.readValue(lines, classOf[YamlListTables])

  configYaml.table.foreach(i =>  {
    println(i)
  //  val df = spark.sqlContext.read.jdbc(loadMysql.getJdbc, s"""(select * from ${i._1}.${i._2}) a """, loadMysql.getProperties)
    //df.write.mode("overwrite").parquet(s"s3a://bigdata-stg/${i._1}.${i._2}")
  }
  )
}