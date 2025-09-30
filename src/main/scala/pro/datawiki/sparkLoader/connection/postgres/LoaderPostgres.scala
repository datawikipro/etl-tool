package pro.datawiki.sparkLoader.connection.postgres

import ch.qos.logback.classic.Level
import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameOriginal
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadata, TableMetadataColumn, TableMetadataType}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, SupportIdMap}
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode.overwriteTable
import pro.datawiki.sparkLoader.dictionaryEnum.{SCDType, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

class LoaderPostgres(configYaml: YamlConfig) extends ConnectionTrait, DatabaseTrait, SupportIdMap, LoggingTrait {
  override def getDataFrameBySQL(sql: String): DataFrame = {
    val startTime = logOperationStart("PostgreSQL SQL query", s"sql: ${sql.take(100)}...")

    try {
      logInfo(s"Executing SQL query on PostgreSQL: ${sql.take(50)}...")

      val df = SparkObject.spark.sqlContext.read
        .option("fetchsize", "10000")
        .jdbc(getJdbc, s"""($sql) a """, getProperties)

      LogMode.debugDF(df)
      logOperationEnd("PostgreSQL SQL query", startTime, s"sql: ${sql.take(50)}...")
      return df

    } catch {
      case e: Exception =>
        logError("PostgreSQL SQL query", e, s"sql: ${sql.take(50)}...")
        throw e
    }
  }

  override def writeDfAppend(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, targetColumns: List[String], uniqueKey: List[String]): Unit = {
    uniqueKey.length match {
      case 0 => super.writeDfAppend(df, tableSchema, tableName, scdType, targetColumns, uniqueKey)
      case _ => {
        val columnsKey = targetColumns::: List("valid_from_dttm","valid_to_dttm")
        val columnsValue = targetColumns::: List("now() valid_from_dttm","to_date('2100','yyyy') valid_to_dttm")
        val uniqueKeyNew = uniqueKey ::: List("valid_to_dttm")
        val cache: TransformationCacheDatabase = TransformationCacheDatabase()
        cache.saveTable(DataFrameOriginal(df),overwriteTable,this)
        runSQL(
          s"""insert into $tableSchema.$tableName(${columnsKey.mkString(",")})
             |select ${columnsValue.mkString(",")} from ${cache.getLocation}
             | ON CONFLICT (${uniqueKeyNew.mkString(",")}) DO NOTHING
             |""".stripMargin )
        runSQL(s"""drop table if exists ${cache.getLocation}""".stripMargin )
      }
    }

  }

  override def writeDf(df: DataFrame, tableSchema: String, tableName: String, writeMode: WriteMode, scdType: SCDType): Unit = {
    val startTime = logOperationStart("PostgreSQL write DataFrame", s"table: ${tableSchema}.${tableName}, mode: $writeMode")
//    scdType match {
//      case _ => {
//        throw NotImplementedException("Method not implemented")
//      }
//    }
    try {
      logInfo(s"Writing DataFrame to PostgreSQL table: ${tableSchema}.${tableName}")

      df.write
        .option("batchsize", "10000")
        .mode(writeMode.toSparkString)
        .jdbc(getJdbc, s"${tableSchema}.${tableName}", getProperties)

      logOperationEnd("PostgreSQL write DataFrame", startTime, s"table: ${tableSchema}.${tableName}")

    } catch {
      case e: Exception =>
        logError("PostgreSQL write DataFrame", e, s"table: ${tableSchema}.${tableName}")
        throw e
    }
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

  override def readDfSchema(tableSchema: String, tableName: String): DataFrame = {
    val df = SparkObject.spark.sqlContext.read
      .option("fetchsize", "10000")
      .jdbc(getJdbc, s"""(select * from ${tableSchema}.${tableName} where 1 = 2) a """, getProperties)
    LogMode.debugDF(df)
    return df
  }

  //  override def getPartitionsForTable(tableSchema:String, tableName: String): List[String] = {
  //    val sql =
  //      s"""select child.relnamespace::regnamespace AS partition_schema, child.relname AS partition_name
  //         |  from pg_inherits
  //         |   join pg_class as child on inhrelid = child.oid
  //         |   join pg_class as parent on inhparent = parent.oid
  //         |where parent.relnamespace::regnamespace || '.' || parent.relname = '${tableName}'
  //         |order by 1,2 desc
  //         |""".stripMargin
  //    val df = getDataFrameBySQL(sql)
  //    var list: List[String] = List.apply()
  //    df.collect().toList.foreach(i => {
  //      list = list.appended(s"${i.get(0).toString}.${i.get(1).toString}")
  //    })
  //
  //    return list
  //  }

  override def runSQL(sql: String): Boolean = {
    val stm = getConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    println(sql)//TODO
    stm.execute(sql)
  }

  def getProperties: Properties = {
    val prop = new java.util.Properties
    prop.setProperty("user", configYaml.login)
    prop.setProperty("password", configYaml.password)
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("fetchsize", "50000") // Increase fetch size for better performance
    prop.setProperty("tcpKeepAlive", "true") // Enable TCP keep alive
    prop.setProperty("ApplicationName", "etl-tool") // Set application name for monitoring
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
    return s"jdbc:postgresql://${db.host}:${db.port}/${db.database}?socketTimeout=300000&loginTimeout=60&connectTimeout=60000"
  }

  private def getServer: YamlServerHost = {
    configYaml.server.replica.foreach(i => {
      if i.validateHost then
        return i
    })
    if configYaml.server.master.validateHost then {
      return configYaml.server.master
    }
    throw NotImplementedException("Server not found")
  }

  def getJdbc: String = {
    if server == null then server = getServer
    return getJdbcDb(server)
  }

  def getDwhConnectionInfo: DwhConnectionInfo = {
    if server == null then server = getServer
    DwhConnectionInfo(
      host = server.host,
      port = server.port,
      database = server.database,
      username = configYaml.login,
      password = configYaml.password
    )
  }

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
        LoaderPostgres.decodeDataType(row.get(1).toString),
        false
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

  override def close(): Unit = {
    if connection == null then return
    connection.close()
  }

  override def encodeDataType(in: TableMetadataType): String = LoaderPostgres.encodeDataType(in)

  override def decodeDataType(in: String): TableMetadataType = LoaderPostgres.decodeDataType(in)

  override def readDf(tableSchema: String, tableName: String): DataFrame = throw Exception("Method not implemented")

  override def readDf(tableSchema: String, tableName: String, partitionName: String): DataFrame = throw Exception("Method not implemented")
}

object LoaderPostgres extends YamlClass {
  def apply(inConfig: String): LoaderPostgres = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderPostgres(configYaml)
  }

  def encodeDataType(in: TableMetadataType): String = {
    return in match {
      case TableMetadataType.Integer => return "int"
      case TableMetadataType.Bigint => return "bigint"
      case TableMetadataType.String => return "String"
      case TableMetadataType.Boolean => return "boolean"
      case TableMetadataType.Varchar => return "character varying"
      case TableMetadataType.Date => return "date"
      case TableMetadataType.DoublePrecision => return "double precision"
      case TableMetadataType.Numeric => return "numeric"
      case TableMetadataType.Real => return "real"
      case TableMetadataType.Text => return "text"
      case TableMetadataType.TimestampWithTimeZone => return "timestamp with time zone"
      case TableMetadataType.TimestampWithoutTimeZone => return "timestamp without time zone"
      case _ => throw NotImplementedException(s"Unsupported PostgreSQL data type encoding: $in")
    }
  }

  def decodeDataType(in: String): TableMetadataType = {
    in match {
      case "bigint" => return TableMetadataType.Bigint
      case "boolean" => return TableMetadataType.Boolean
      case "character varying" => return TableMetadataType.Varchar
      case "date" => return TableMetadataType.Date
      case "double precision" => return TableMetadataType.DoublePrecision
      case "integer" => return TableMetadataType.Integer
      case "int" => return TableMetadataType.Integer
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
      case "String" => return TableMetadataType.String
      case "UInt8" => return TableMetadataType.Uint
      case _ => {
        throw NotImplementedException(s"Unsupported PostgreSQL data type decoding: '$in'")
      }
    }
  }
}