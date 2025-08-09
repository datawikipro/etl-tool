package pro.datawiki.sparkLoader.connection.postgres

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataColumn, TableMetadataType}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DataWarehouseTrait, DatabaseTrait, SupportIdMap, WriteMode}
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

class LoaderPostgres(configYaml: YamlConfig) extends ConnectionTrait, DatabaseTrait,SupportIdMap, DataWarehouseTrait, LazyLogging {
  override def getDataFrameBySQL(sql: String): DataFrame = {
    try {
      val df = SparkObject.spark.sqlContext.read
        .option("fetchsize", "10000")
        .jdbc(getJdbc, s"""($sql) a """, getProperties)
      LogMode.debugDF(df)
      return df
    } catch {
      case e: Exception => {
        throw e
      }
    }
  }

  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    df.write
      .option("batchsize", "10000")
      .mode(writeMode.toSparkString)
      .jdbc(getJdbc, location, getProperties)
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
    val df = SparkObject.spark.sqlContext.read
      .option("fetchsize", "10000")
      .jdbc(getJdbc, s"""(select * from $location where 1 = 2) a """, getProperties)
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
    prop.setProperty("fetchsize", "10000")
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
    if configYaml.server.master.validateHost then {
      return configYaml.server.master
    }
    throw Exception()
  }

  def getJdbc: String = {
    if server == null then server = getServer
    return getJdbcDb(server)
  }

  override def truncateTable(tableName: String): Boolean = throw Exception()

  private def getTableColumns(tableSchema: String, tableName: String): List[TableMetadataColumn] = {
    val sql =
      s"""select column_name,
         |       data_type
         |  from information_schema.columns
         | where table_schema = '$tableSchema'
         |   and table_name   = '$tableName'
         | order by ordinal_position
         |""".stripMargin
    val df = getDataFrameBySQL(sql)
    try {
      val a = df.collect().toList
      val b = a.map(row => TableMetadataColumn(
        row.get(0).toString,
        LoaderPostgres.decodeDataType(row.get(1).toString)
      ))
      return b
    }
    catch {
      case e: Exception => {
        throw e
      }
    }
  }

  private def getPrimaryKey(tableSchema: String, tableName: String): List[String] = {
    val sql =
      s"""select column_name
         |from information_schema.table_constraints tc
         |         JOIN information_schema.key_column_usage AS kcu using (constraint_name, constraint_schema, table_name)
         |where constraint_type = 'PRIMARY KEY'
         |  and constraint_schema = '$tableSchema'
         |  and table_name = '$tableName'
         |order by ordinal_position
         |""".stripMargin
    val df = getDataFrameBySQL(sql)

    return df.collect().toList.map(row => row.get(0).toString)
  }

  override def getTableMetadata(tableSchema: String, tableName: String): TableMetadata = {
    val table: TableMetadata = TableMetadata(
      columns = getTableColumns(tableSchema, tableName),
      primaryKey = getPrimaryKey(tableSchema, tableName)
    )

    return table
  }
  
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

  private def encodeDataType(in: TableMetadataType): String = throw Exception()

  private def decodeDataType(in: String): TableMetadataType = {
    in match {
      case "bigint" => return TableMetadataType.Bigint
      case "boolean" => return TableMetadataType.Boolean
      case "character varying" => return TableMetadataType.Varchar
      case "date" => return TableMetadataType.Date
      case "double precision" => return TableMetadataType.DoublePrecision
      case "integer" => return TableMetadataType.Integer
      case "numeric" => return TableMetadataType.Numeric
      case "real" => return TableMetadataType.Real
      case "text" => return TableMetadataType.Text
      case "ARRAY" => return TableMetadataType.Text
      case "jsonb" => return TableMetadataType.Text
      case "uuid" => return TableMetadataType.Text
      case "timestamp with time zone" => return TableMetadataType.TimestampWithTimeZone
      case "timestamp without time zone" => return TableMetadataType.TimestampWithoutTimeZone
      case "USER-DEFINED" => return TableMetadataType.Text
      case "bytea" => return TableMetadataType.Text
      case _ => {
        throw Exception()
      }
    }
  }
}