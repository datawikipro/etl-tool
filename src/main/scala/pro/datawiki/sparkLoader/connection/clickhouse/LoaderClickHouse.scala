package pro.datawiki.sparkLoader.connection.clickhouse

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataType}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, DatabaseTrait, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

class LoaderClickHouse(configYaml: YamlConfig) extends ConnectionTrait, DatabaseTrait, DataWarehouseTrait {

  override def getDataFrameBySQL(sql: String): DataFrame = {
    val localDf = SparkObject.spark.sqlContext.read.jdbc(getJdbc, s"""($sql) a """, getProperties)
    LogMode.debugDF(localDf)
    return localDf
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

  override def truncateTable(tableName: String): Boolean = {
    val stm = getConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stm.execute(s"truncate table $tableName")
  }


  override def writeDf(df: DataFrame, tableName: String, writeMode: WriteMode): Unit = {
    writeMode match
      case WriteMode.overwrite => {
        truncateTable(tableName)
        df.write.mode(WriteMode.append.toSparkString).jdbc(getJdbc, tableName, getProperties)
      }
      case WriteMode.append => {
        df.write.mode(WriteMode.append.toSparkString).jdbc(getJdbc, tableName, getProperties)
      }
      case _ => throw Exception()
  }
  
  override def readDf(location: String): DataFrame = throw Exception()

  var connection: Connection = null

  @Override
  override def getConnection: Connection = {
    if connection == null then connection = DriverManager.getConnection(getJdbc, getProperties)
    return connection
  }

  override def getPartitionsForTable(tableName: String): List[String] = {
    throw Exception()
  }

  override def close(): Unit = {}

  override def readDfSchema(location: String): DataFrame = throw Exception()

  override def runSQL(in: String): Boolean = throw Exception()

  override def getTableMetadata(tableSchema: String, tableName: String): TableMetadata = ???
}

object LoaderClickHouse extends YamlClass {
  def apply(inConfig: String): LoaderClickHouse = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderClickHouse(configYaml)
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
      case _ => throw Exception()
    }
  }

  def decodeDataType(in: String): TableMetadataType = throw Exception()
}