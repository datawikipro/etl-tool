package pro.datawiki.sparkLoader.connection.clickhouse

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, DatabaseTrait, WriteMode}
import pro.datawiki.sparkLoader.{SparkObject, YamlClass}

import java.util.Properties

class LoaderClickHouse(configYaml: YamlConfig) extends ConnectionTrait, DatabaseTrait,DataWarehouseTrait {

  override def getDataFrameBySQL(sql: String): DataFrame = {
    SparkObject.spark.sqlContext.read.jdbc(getJdbc, s"""($sql) a """, getProperties)
  }

  override def insertCCdToIdmap(df: DataFrame,
                                domainName: String,
                                tenantName: String,
                                hasRk: Boolean): Unit = throw Exception()

  override def getIdmapDataFrame(domainName: String, tenantName: String): DataFrame = throw Exception()

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
    prop.setProperty("sslmode", "STRICT")//"NONE"
    prop.setProperty("sslrootcert", "")

    return prop
  }

  private def getJdbcDb(db: YamlServerHost): String = {
//    val res = s"jdbc:clickhouse://${db.host}:${db.port}/${db.database}?ssl=true&sslmode=STRICT"
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

  override def writeDf(location: String, df: DataFrame, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).jdbc(getJdbc, location, getProperties)
  }

  override def readDf(location: String, segmentName: String): DataFrame = throw Exception()

  override def readDf(location: String): DataFrame = throw Exception()

  override def writeDf(location: String, df: DataFrame, columnsLogicKey: List[String], columns: List[String], writeMode: WriteMode): Unit = throw Exception()

  override def close(): Unit = {}
}

object LoaderClickHouse extends YamlClass {
  def apply(inConfig: String): LoaderClickHouse = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderClickHouse(configYaml)
  }
}