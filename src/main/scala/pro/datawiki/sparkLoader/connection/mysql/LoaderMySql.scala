package pro.datawiki.sparkLoader.connection.mysql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, SparkSession}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.{DatabaseTrait, ConnectionTrait}

import java.nio.file.{Files, Paths}
import java.util.Properties

class LoaderMySql(configYaml: YamlConfig) extends ConnectionTrait,DatabaseTrait {

  override def getDataFrameBySQL(sql: String): DataFrame = {
    SparkObject.spark.sqlContext.read.jdbc(getJdbc, s"""($sql) a """, getProperties)
  }
  
  def getProperties:Properties = {
    val prop = new java.util.Properties
    prop.setProperty("user", configYaml.login)
    prop.setProperty("password", configYaml.password)
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    return prop
  }

  private def getJdbcDb(db:YamlServerHost):String = {
    return s"jdbc:mysql://${db.host}:${db.port}"
  }

  def getJdbc:String = {
    configYaml.server.replica.foreach(i=> {
      return getJdbcDb(i)
    })
    return getJdbcDb(configYaml.server.master)
  }
}

object LoaderMySql{
  def apply(inConfig:String):LoaderMySql = {

    val lines: String = Files.readString(Paths.get(inConfig))

    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val configYaml: YamlConfig = mapper.readValue(lines, classOf[YamlConfig])
    return new LoaderMySql(configYaml)
  }
}