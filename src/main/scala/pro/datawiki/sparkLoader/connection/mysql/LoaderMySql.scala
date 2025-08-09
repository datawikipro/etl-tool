package pro.datawiki.sparkLoader.connection.mysql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.jcraft.jsch.{JSch, Session}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, md5}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.configuration.yamlConfigTarget.YamlConfigTargetColumn
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataType}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, WriteMode}
import pro.datawiki.yamlConfiguration.YamlClass

import java.nio.file.{Files, Paths}
import java.sql.Connection
import java.util.Properties

class LoaderMySql(configYaml: YamlConfig) extends ConnectionTrait, DatabaseTrait {

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
  override def getPartitionsForTable(tableName: String): List[String] = {
    throw Exception()
  }
  @Override
  def getConnection: Connection = {
    throw Exception()
  }

  override def readDf(location: String): DataFrame = throw Exception()

  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = throw Exception()

  override def close(): Unit = {
    if session != null then {
      session.disconnect()
    }
  }

  override def readDfSchema(in: String): DataFrame = throw Exception()

  override def runSQL(in: String): Boolean = throw Exception()

  override def truncateTable(tableName: String): Boolean = throw Exception()

  override def getTableMetadata(tableSchema: String, tableName: String): TableMetadata = throw Exception()
  
}

object LoaderMySql extends YamlClass {
  def apply(inConfig: String): LoaderMySql = {

    val lines: String = Files.readString(Paths.get(inConfig))
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val configYaml: YamlConfig = mapper.readValue(lines, classOf[YamlConfig])
    return new LoaderMySql(configYaml)
  }

   def encodeDataType(in: TableMetadataType): String = throw Exception()

   def decodeDataType(in: String): TableMetadataType = throw Exception()
}