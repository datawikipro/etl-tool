package pro.datawiki.sparkLoader.connection.clickhouse

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataType}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.{SCDType, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

class LoaderClickHouse(sourceName: String, configYaml: YamlConfig) extends ConnectionTrait, DatabaseTrait, LoggingTrait {

  override def getDataFrameBySQL(sql: String): DataFrame = {
    val startTime = logOperationStart("ClickHouse SQL query", s"source: $sourceName, sql: ${sql.take(100)}...")

    try {
      logInfo(s"Executing SQL query on ClickHouse ($sourceName): ${sql.take(50)}...")

      val localDf = SparkObject.spark.sqlContext.read.jdbc(getJdbc, s"""($sql) a """, getProperties)
      LogMode.debugDF(localDf)

      logOperationEnd("ClickHouse SQL query", startTime, s"source: $sourceName")
      return localDf

    } catch {
      case e: Exception =>
        logError("ClickHouse SQL query", e, s"source: $sourceName, sql: ${sql.take(50)}...")
        throw e
    }
  }

  def getProperties: Properties = {
    val prop = new java.util.Properties
    prop.setProperty("user", configYaml.login)
    prop.setProperty("password", configYaml.password)
    //    prop.setProperty("sslmode", "strict") // NONE to trust all servers; STRICT for trusted only
    prop.setProperty("sslmode", "NONE") // NONE to trust all servers; STRICT for trusted only
    prop.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")

    prop.setProperty("ssl", "true")
    prop.setProperty("sslcert", "")
    prop.setProperty("sslkey", "")
    prop.setProperty("sslmode", "STRICT") //"NONE"
    prop.setProperty("sslrootcert", "")

    return prop
  }

  private def getJdbcDb(db: YamlServerHost): String = {
    val res = s"jdbc:clickhouse://${db.host}:${db.port}/${db.database}"
    return res
  }

  def getJdbc: String = {
    if configYaml.server.replica != null then {
      configYaml.server.replica.foreach(i => {
        return getJdbcDb(i)
      })
    }
    return getJdbcDb(configYaml.server.master)
  }
  
  override def writeDf(df: DataFrame, tableSchema: String, tableName: String, writeMode: WriteMode, scdType: SCDType): Unit =  {
    writeMode match
      case WriteMode.overwriteTable => {
        df.write.mode(WriteMode.append.toSparkString).jdbc(getJdbc, tableName, getProperties)
      }
      case WriteMode.append => {
        df.write.mode(WriteMode.append.toSparkString).jdbc(getJdbc, tableName, getProperties)
      }
      case _ => throw NotImplementedException("Method not implemented")
  }

  var connection: Connection = null
  
  private def getConnection: Connection = {
    if connection == null then connection = DriverManager.getConnection(getJdbc, getProperties)
    return connection
  }

  override def close(): Unit = {
    if getConnection == null then return
    getConnection.close()
    connection = null
  }

  override def runSQL(in: String): Boolean = {
    try {
      val stm = getConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val result = stm.execute(in)
      stm.close()
      logInfo(s"Successfully executed SQL: $in")
      return true
    } catch {
      case e: Exception => {
        logError("SQL execution", e, s"SQL: $in")
        throw e
      }
    }
  }

  override def encodeDataType(in: TableMetadataType): String = throw NotImplementedException("Method not implemented")

  override def decodeDataType(in: String): TableMetadataType = throw NotImplementedException("Method not implemented")

  override def readDf(tableSchema: String, tableName: String): DataFrame = throw NotImplementedException("Method not implemented")

  override def readDf(tableSchema: String, tableName: String, partitionName: String): DataFrame = throw NotImplementedException("Method not implemented")

  override def readDfSchema(tableSchema: String, tableName: String): DataFrame = throw NotImplementedException("Method not implemented")
}

object LoaderClickHouse extends YamlClass {
  def apply(sourceName: String, inConfig: String): LoaderClickHouse = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderClickHouse(sourceName, configYaml)
  }

  def encodeDataType(in: TableMetadataType): String = {
    return in match {
      case TableMetadataType.Integer => return "Int32"
      case TableMetadataType.Bigint => return "Int64"
      case TableMetadataType.String => return "String"
      case TableMetadataType.Boolean => return "UInt8"
      case TableMetadataType.Varchar => return "String"
      case TableMetadataType.Date => return "Date"
      case TableMetadataType.DoublePrecision => return "Float64"
      case TableMetadataType.Numeric => return "Float64"
      case TableMetadataType.Real => return "Float32"
      case TableMetadataType.Text => return "String"
      case TableMetadataType.TimestampWithTimeZone => return "DateTime"
      case TableMetadataType.TimestampWithoutTimeZone => return "DateTime"
      case _ => throw NotImplementedException("Method not implemented")
    }
  }

  def encodeIsNullable(in: Boolean, inType: String): String = {
    return in match {
      case true => return s"Nullable(${inType})"
      case false => return inType
    }
  }

  def decodeDataType(in: String): TableMetadataType = throw NotImplementedException("Method not implemented")
}