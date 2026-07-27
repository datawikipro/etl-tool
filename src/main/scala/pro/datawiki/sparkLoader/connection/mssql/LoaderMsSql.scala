package pro.datawiki.sparkLoader.connection.mssql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.connection.databaseTrait.TableMetadataType
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, SCDType, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.nio.file.{Files, Paths}
import java.sql.Connection
import java.util.Properties

class LoaderMsSql(configYaml: YamlConfig, configLocation: String) extends ConnectionTrait with DatabaseTrait with LoggingTrait {
  private val _configLocation: String = configLocation

  logInfo(s"Creating MS SQL Server connection for host=${configYaml.host}, database=${configYaml.database}")

  def getProperties: Properties = {
    val prop = new Properties()
    val username = if (configYaml.user != null && configYaml.user.nonEmpty) configYaml.user else configYaml.login
    prop.setProperty("user", username)
    prop.setProperty("password", configYaml.password)
    prop.setProperty("driver", if (configYaml.driver != null && configYaml.driver.nonEmpty) configYaml.driver else "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    prop
  }

  def getJdbc: String = {
    val encrypt = if (configYaml.options != null) configYaml.options.getOrElse("encrypt", "false") else "false"
    val trustCert = if (configYaml.options != null) configYaml.options.getOrElse("trustServerCertificate", "true") else "true"
    s"jdbc:sqlserver://${configYaml.host}:${configYaml.port};databaseName=${configYaml.database};encrypt=$encrypt;trustServerCertificate=$trustCert;"
  }

  override def getDataFrameBySQL(sql: String): DataFrame = {
    val startTime = logOperationStart("MS SQL query", s"sql: ${sql.take(100)}...")
    try {
      logInfo(s"Executing SQL query on MS SQL Server: ${sql.take(50)}...")
      val df = SparkObject.spark.sqlContext.read
        .option("fetchsize", "10000")
        .jdbc(getJdbc, s"($sql) a", getProperties)

      LogMode.debugDF(df)
      logOperationEnd("MS SQL query", startTime, s"sql: ${sql.take(50)}...")
      df
    } catch {
      case e: Exception =>
        logError("MS SQL query", e, s"sql: ${sql.take(50)}...")
        throw e
    }
  }

  override def getConnection: Connection = throw NotImplementedException("Method not implemented")

  override def close(): Unit = {
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = ConnectionEnum.mssql

  override def getConfigLocation(): String = _configLocation

  override def encodeDataType(in: TableMetadataType): String = throw NotImplementedException("Method not implemented")
  override def decodeDataType(in: String): TableMetadataType = throw NotImplementedException("Method not implemented")
  override def readDf(tableSchema: String, tableName: String): DataFrame = throw NotImplementedException("Method not implemented")
  override def readDf(tableSchema: String, tableName: String, partitionName: String): DataFrame = throw NotImplementedException("Method not implemented")
  override def runSQL(in: String): Boolean = throw NotImplementedException("Method not implemented")
  override def writeDf(df: DataFrame, tableSchema: String, tableName: String, writeMode: WriteMode, scdType: SCDType, partitionBy: List[(String, String)]): Unit = throw NotImplementedException("Method not implemented")
  override def readDfSchema(tableSchema: String, tableName: String): DataFrame = throw NotImplementedException("Method not implemented")
  override def setTemporaryTable(tableName: String, sql: String): Boolean = throw NotImplementedException("Method not implemented")
}

object LoaderMsSql extends YamlClass {
  def apply(inConfig: String): LoaderMsSql = {
    val lines: String = Files.readString(Paths.get(inConfig))
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val configYaml: YamlConfig = mapper.readValue(lines, classOf[YamlConfig])
    new LoaderMsSql(configYaml, inConfig)
  }

  def encodeDataType(in: TableMetadataType): String = throw NotImplementedException("Method not implemented")
  def decodeDataType(in: String): TableMetadataType = throw NotImplementedException("Method not implemented")
}
