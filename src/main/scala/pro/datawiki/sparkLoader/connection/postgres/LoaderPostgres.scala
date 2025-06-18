package pro.datawiki.sparkLoader.connection.postgres

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, DatabaseTrait, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

class LoaderPostgres(configYaml: YamlConfig) extends ConnectionTrait, DatabaseTrait, DataWarehouseTrait, LazyLogging {
  override def getDataFrameBySQL(sql: String): DataFrame = {
    val df = SparkObject.spark.sqlContext.read.jdbc(getJdbc, s"""($sql) a """, getProperties)
    LogMode.debugDF(df)
    return df
  }

  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).jdbc(getJdbc, location, getProperties)
  }

  override def generateIdMap(inTable: String, domain: String, systemCode: String): Boolean = {
    val stm = getConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    val sql =
      s"""insert into idmap.${domain}(ccd, system_code)
         |with id as (
         |    select ccd
         |      from idmap.${domain}
         |     where system_code = '${systemCode}')
         |select data.ccd,'${systemCode}'
         |  from ${inTable} as data
         |  left join id using (ccd)
         | where id.ccd is null
         |   """.stripMargin
    stm.execute(sql)
    return true
  }

  override def mergeIdMap(inTable: String, domain: String, inSystemCode: String, outSystemCode: String): Boolean = {
    val stm = getConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    val sql =
      s"""insert into idmap.${domain}(ccd, system_code, rk)
         |with  in_idmap as (select ccd, rk from idmap.${domain} where system_code = '${inSystemCode}'),
         |     out_idmap as (select ccd     from idmap.${domain} where system_code = '${outSystemCode}')
         |select data.out_ccd,'${outSystemCode}', max(in_idmap.rk)
         |  from ${inTable} as data
         |        join  in_idmap on in_ccd = in_idmap.ccd
         |   left join out_idmap on out_ccd = out_idmap.ccd
         | where out_idmap.ccd is null
         | group by data.out_ccd
         |having count(distinct in_idmap.rk) = 1
         |   """.stripMargin
    stm.execute(sql)
    return true
  }

  override def readDf(location: String): DataFrame = throw Exception()

  override def readDfSchema(location: String): DataFrame = {
    val df = SparkObject.spark.sqlContext.read.jdbc(getJdbc, s"""(select * from $location where 1 = 2) a """, getProperties)
    LogMode.debugDF(df)
    return df
  }

  override def getPartitionsForTable(tableName: String): List[String] = {
    val sql =
      s"""select child.relnamespace::regnamespace AS partition_schema, child.relname AS partition_name
         |  from pg_inherits
         |   join pg_class as child on inhrelid = child.oid
         |   join pg_class as parent on inhparent = parent.oid
         |where parent.relnamespace::regnamespace || '.' || parent.relname = '${tableName}'
         |order by 1,2 desc
         |""".stripMargin
    val df = getDataFrameBySQL(sql)
    var list: List[String] = List.apply()
    df.collect().toList.foreach(i => {
      list = list.appended(s"${i.get(0).toString}.${i.get(1).toString}")
    })

    return list
  }

  override def runSQL(sql: String): Boolean = {
    val stm = getConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stm.execute(sql)
  }

  def getProperties: Properties = {
    val prop = new java.util.Properties
    prop.setProperty("user", configYaml.login)
    prop.setProperty("password", configYaml.password)
    prop.setProperty("driver", "org.postgresql.Driver")
    return prop
  }

  var connection: Connection = null

  var server: YamlServerHost = null

  @Override
  def getConnection: Connection = {
    if connection == null then connection = DriverManager.getConnection(getJdbc, getProperties)
    return connection
  }

  private def getJdbcDb(db: YamlServerHost): String = {
    return s"jdbc:postgresql://${db.host}:${db.port}/${db.database}?socketTimeout=12000"
  }

  private def getServer: YamlServerHost = {
    configYaml.server.replica.foreach(i => {
      if i.validateHost then
        return i
    })
    if configYaml.server.master.validateHost then
      return configYaml.server.master
    throw Exception()
  }

  def getJdbc: String = {
    if server == null then server = getServer
    return getJdbcDb(server)
  }

  override def truncateTable(tableName: String): Boolean = throw Exception()

  override def close(): Unit = {
    if connection == null then return
    connection.close()
  }
}

object LoaderPostgres extends YamlClass {
  def apply(inConfig: String): LoaderPostgres = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderPostgres(configYaml)
  }
}