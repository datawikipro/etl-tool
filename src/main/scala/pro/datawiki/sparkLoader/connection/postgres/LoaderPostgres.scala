package pro.datawiki.sparkLoader.connection.postgres

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, SparkSession}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}

import java.nio.file.{Files, Paths}
import java.util.Properties
import org.apache.spark.sql.functions.{col, concat_ws, lit}

class LoaderPostgres(configYaml: YamlConfig) extends ConnectionTrait,DatabaseTrait {

  override def getDataFrameBySQL(sql: String): DataFrame = {
    SparkObject.spark.sqlContext.read.jdbc(getJdbc, s"""($sql) a """, getProperties)
  }

  override def insertCCdToIdmap(df:DataFrame,domainName:String,tenantName:String): Unit = {
    df.select(lit(tenantName).as("tenant"),col("ccd")).write.mode("append").jdbc(getJdbc,domainName,getProperties)
  }
  
  override def getIdmapDataFrame(domainName:String, tenantName:String):DataFrame = {
    return getDataFrameBySQL(s"select ccd, tenant, rk from $domainName")
  }

  def getProperties:Properties = {
    val prop = new java.util.Properties
    prop.setProperty("user", configYaml.login)
    prop.setProperty("password", configYaml.password)
    prop.setProperty("driver", "org.postgresql.Driver")
    return prop
  }

  private def getJdbcDb(db:YamlServerHost):String = {
    return s"jdbc:postgresql://${db.host}:${db.port}/${db.database}"
  }

  def getJdbc:String = {
    if configYaml.server.replica != null then {
      configYaml.server.replica.foreach(i => {
        return getJdbcDb(i)
      })
    }
    return getJdbcDb(configYaml.server.master)
  }
}

object LoaderPostgres{
  def apply(inConfig:String):LoaderPostgres = {

    val lines: String = Files.readString(Paths.get(inConfig))

    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val configYaml: YamlConfig = mapper.readValue(lines, classOf[YamlConfig])
    return new LoaderPostgres(configYaml)
  }
}