package pro.datawiki.sparkLoader.connection.mysql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.jcraft.jsch.{JSch, Session}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, md5}
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.yamlConfigTargetDatabase.YamlConfigTargetColumn
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataType}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, SCDType, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.nio.file.{Files, Paths}
import java.sql.Connection
import java.util.Properties

class LoaderMySql(configYaml: YamlConfig, configLocation: String) extends ConnectionTrait, DatabaseTrait {
  private val _configLocation: String = configLocation
  
  logInfo("Creating MySQL connection")

  override def getDataFrameBySQL(sql: String): DataFrame = {
    val df = SparkObject.spark.sqlContext.read.jdbc(getJdbc, s"""($sql) a """, getProperties)
    LogMode.debugDF(df)
    return df
  }

  def getProperties: Properties = {
    val prop = new java.util.Properties
    prop.setProperty("user", configYaml.login)
    prop.setProperty("password", configYaml.password)
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    return prop
  }

  var session: Session = null

  private def getJdbcDb(db: YamlServerHost, gateway: YamlGateway): String = {
    if gateway == null then {
      return s"jdbc:mysql://${db.host}:${db.port}/${db.database}"
    }
    var jsch: JSch = new JSch();
    session = jsch.getSession(gateway.ssh.user, gateway.ssh.host, gateway.ssh.port.toInt)
    jsch.addIdentity(gateway.ssh.keyLocation)
    JSch.setConfig("PubkeyAcceptedAlgorithms", JSch.getConfig("PubkeyAcceptedAlgorithms") + ",ssh-rsa")
    session.setConfig("StrictHostKeyChecking", "no")
    session.connect()
    val por = session.setPortForwardingL(0, db.host, db.port.toInt)
    val url = "jdbc:mysql://localhost:" + por + s"/${db.database}"
    return url

  }


  def getJdbc: String = {
    configYaml.server.replica.foreach(f = i => {
      return getJdbcDb(i, configYaml.gateway)
    })
    return getJdbcDb(configYaml.server.master, configYaml.gateway)
  }

  @Override
  def getConnection: Connection = {
    throw NotImplementedException("Method not implemented")
  }

  override def close(): Unit = {
    if session != null then {
      session.disconnect()
    }
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = {
    ConnectionEnum.mysql
  }

  override def getConfigLocation(): String = {
    _configLocation
  }


  override def encodeDataType(in: TableMetadataType): String = throw NotImplementedException("Method not implemented")

  override def decodeDataType(in: String): TableMetadataType = throw NotImplementedException("Method not implemented")

  override def readDf(tableSchema: String, tableName: String): DataFrame =throw NotImplementedException("Method not implemented")

  override def readDf(tableSchema: String, tableName: String, partitionName: String): DataFrame = throw NotImplementedException("Method not implemented")

  override def runSQL(in: String): Boolean = throw NotImplementedException("Method not implemented")

  override def writeDf(df: DataFrame, tableSchema: String, tableName: String, writeMode: WriteMode, scdType: SCDType): Unit = throw NotImplementedException("Method not implemented")

  override def readDfSchema(tableSchema: String, tableName: String): DataFrame =throw NotImplementedException("Method not implemented")
}

object LoaderMySql extends YamlClass {
  def apply(inConfig: String): LoaderMySql = {

    val lines: String = Files.readString(Paths.get(inConfig))
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val configYaml: YamlConfig = mapper.readValue(lines, classOf[YamlConfig])
    return new LoaderMySql(configYaml, inConfig)
  }

  def encodeDataType(in: TableMetadataType): String = throw NotImplementedException("Method not implemented")

  def decodeDataType(in: String): TableMetadataType = throw NotImplementedException("Method not implemented")
}