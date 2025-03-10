package pro.datawiki.sparkLoader.connection.postgres

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, DatabaseTrait, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject, YamlClass}

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import com.typesafe.scalalogging.LazyLogging
import pro.datawiki.sparkLoader.transformation.TransformationCache

class LoaderPostgres(configYaml: YamlConfig) extends ConnectionTrait, DatabaseTrait, DataWarehouseTrait, LazyLogging {
  override def getDataFrameBySQL(sql: String): DataFrame = {
    val df = SparkObject.spark.sqlContext.read.jdbc(getJdbc, s"""($sql) a """, getProperties)
    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    return df
  }

  override def readDf(location: String, segmentName: String): DataFrame = throw Exception()

  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    df.write.mode(writeMode.toString).jdbc(getJdbc, location, getProperties)
  }

  @Override
  override def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit ={
    writeDf(df, location, writeMode)
  } 
    
  override def readDf(location: String): DataFrame = throw Exception()

  override def writeDf(df: DataFrame, location: String, columnsLogicKey: List[String], columns: List[String], writeMode: WriteMode): Unit = {
    val cache = new TransformationCache(this)
    cache.saveTable(df)

    var orList: List[String] = List.apply()
    var joinList: List[String] = List.apply()

    columns.foreach(i => orList = orList.appended(s"   or src.${i} <> tgt.${i}"))
    columnsLogicKey.foreach(i => joinList = joinList.appended(s"src.${i} = tgt.${i}"))
    val sql: String =
      s"""create table ${cache.getLocation}_2 as
         |with src as (select * from ${location} where valid_to_dttm = to_date('2100','yyyy')),
         |     tgt as (select * from ${cache.getLocation})
         |select case when src.${columnsLogicKey.head} is not null then 'Update' else 'Insert' end as update_command,
         |       now() as new_date,
         |       tgt.*
         |  from tgt
         |  left join src on ${joinList.mkString(" and ")}
         |where src.${columnsLogicKey.head} is null
         |${orList.mkString("\n")}
         |""".stripMargin

    val stm = getConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stm.execute(sql)
    val sql2: String =
      s"""
         |update ${location} tgt
         |   set valid_to_dttm = new_date - interval '1 microsecond'
         |  from ${cache.getLocation}_2 src
         | where tgt.valid_to_dttm = to_date('2100','yyyy')
         |   and src.update_command = 'Update'
         |   and ${joinList.mkString(" and ")}""".stripMargin
    stm.execute(sql2)

    val sql3: String =
      s"""
         |insert into ${location}(${(columnsLogicKey ::: columns).mkString(", ")},valid_from_dttm,valid_to_dttm)
         |select ${(columnsLogicKey ::: columns).mkString(", ")}, new_date as valid_from_dttm,to_date('2100','yyyy') as valid_to_dttm
         |  from ${cache.getLocation}_2
         |""".stripMargin
    stm.execute(sql3)
    stm.execute(s"""drop table ${cache.getLocation}_2""")
    stm.execute(s"""drop table ${cache.getLocation}""")

  }

  override def insertCCdToIdmap(df: DataFrame,
                                domainName: String,
                                tenantName: String,
                                hasRk: Boolean): Unit = {
    val tempNameDf = s"""${domainName}_${tenantName}_df"""
    val tempNameMinus = s"""${domainName}_${tenantName}_dfMinus"""

    df.createTempView(tempNameDf)
    getIdmapDataFrame(domainName = domainName, tenantName = tenantName).createTempView(tempNameMinus)
    val colList = hasRk match
      case true => List.apply(s"'$tenantName' as tenant", "df.ccd", "df.rk")
      case false => List.apply(s"'$tenantName' as tenant", "df.ccd")
    val sql =
      s"""select ${colList.mkString(", ")}
         | from $tempNameDf df
         | left join $tempNameMinus df_minus using (ccd)
         |where df_minus.rk is null
         |""".stripMargin

    val df2 = SparkObject.spark.sql(sql)
    df2.show()
    df2.write.mode("append").jdbc(getJdbc, domainName, getProperties)
  }

  override def getIdmapDataFrame(domainName: String, tenantName: String): DataFrame = {
    return getDataFrameBySQL(s"""select ccd, tenant, rk from $domainName where tenant = '$tenantName'""")
  }
  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit =  throw Exception()

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
    return s"jdbc:postgresql://${db.host}:${db.port}/${db.database}"
  }

  private def getServer: YamlServerHost = {
    configYaml.server.replica.foreach(i => {
      if i.validateHost then return i
    })
    if configYaml.server.master.validateHost then return configYaml.server.master
    throw Exception()
  }

  def getJdbc: String = {
    if server == null then server = getServer
    return getJdbcDb(server)
  }

  override def close(): Unit = {
    getConnection.close()
  }
}

object LoaderPostgres extends YamlClass {
  def apply(inConfig: String): LoaderPostgres = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderPostgres(configYaml)
  }
}