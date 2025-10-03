package pro.datawiki.sparkLoader.connection.postgres

import org.apache.spark.sql.DataFrame
import pro.datawiki.datawarehouse.DataFrameOriginal
import pro.datawiki.exception.NotImplementedException
import pro.datawiki.sparkLoader.connection.databaseTrait.{TableMetadataColumn, TableMetadataType}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, DatabaseTrait, SupportIdMap}
import pro.datawiki.sparkLoader.context.ApplicationContext
import pro.datawiki.sparkLoader.dictionaryEnum.SCDType.{SCD_2, SCD_3}
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode.overwriteTable
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, SCDType, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.transformation.TransformationCacheDatabase
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.yamlConfiguration.YamlClass

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

class LoaderPostgres(configYaml: YamlConfig, configLocation: String) extends ConnectionTrait, DatabaseTrait, SupportIdMap, LoggingTrait {
  private val _configLocation: String = configLocation
  
  logInfo("Creating PostgreSQL connection")
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
        val columnsKey = targetColumns ::: List("valid_from_dttm", "valid_to_dttm","run_id")
        val columnsValue = targetColumns ::: List("now() valid_from_dttm", "to_date('2100','yyyy') valid_to_dttm",org.apache.spark.sql.functions.lit(s"'${ApplicationContext.getRunId}'"))
        val uniqueKeyNew = uniqueKey ::: List("valid_to_dttm")

        val cache: TransformationCacheDatabase = TransformationCacheDatabase()
        cache.saveTable(DataFrameOriginal(df), overwriteTable, this)
        runSQL(
          s"""insert into $tableSchema.$tableName(${columnsKey.mkString(",")})
             |select ${columnsValue.mkString(",")} from ${cache.getLocation}
             | ON CONFLICT (${uniqueKeyNew.mkString(",")}) DO NOTHING
             |""".stripMargin)
        runSQL(s"""drop table if exists ${cache.getLocation}""".stripMargin)
      }
    }

  }

  override def writeDfMerge(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, columns: List[String], uniqueKey: List[String]): Unit = {
    val startTime = logOperationStart("PostgreSQL merge operation", s"table: $tableSchema.$tableName")
    // Create cache for temporary table
    val cache: TransformationCacheDatabase = TransformationCacheDatabase()

    try {
      if (uniqueKey.isEmpty) {
        throw new IllegalArgumentException("Unique key is required for merge operation")
      }

      logInfo(s"Starting merge operation for PostgreSQL table: $tableSchema.$tableName")

      cache.saveTable(DataFrameOriginal(df), WriteMode.overwriteTable, this)
      var systemColumns: List[(String, String)] = List.empty
      scdType match {
        case SCD_3 => {
          systemColumns = List(
            ("valid_from_dttm", "now()"),
            ("valid_to_dttm", "to_date('2100','yyyy')"),
            ("run_id",s"""'${ApplicationContext.getRunId}'"""))

        }
        case fs => {
          throw Exception()
        }
      }

      val columnsWithChanges = columns.filter(col => !uniqueKey.toSet.contains(col))
      val columnsWithoutChanges = getTableColumns(tableSchema, tableName)
        .map(col => col.column_name)
        .filter(col => !uniqueKey.contains(col) && !columnsWithChanges.contains(col) && !systemColumns.map(col => col._1).contains(col))

      val deltaTableSql =
        s"""
           |select ${(uniqueKey ::: columnsWithChanges).mkString(",")} from ${cache.getLocation}
           |except
           |select ${(uniqueKey ::: columnsWithChanges).mkString(",")} from $tableSchema.$tableName
           |where valid_to_dttm = to_date('2100','yyyy')
           |""".stripMargin

      val createDeltaTableSql = s"create table ${cache.getLocation}_2 as $deltaTableSql"
      runSQL(createDeltaTableSql)

      // Calculate plan table
      val joinString = uniqueKey.map(i => s"src.$i = tgt.$i").mkString(" and ")

      val orList = columnsWithChanges.map(col => {
        val columnMetadata = getTableColumns(tableSchema, tableName).find(_.column_name == col)
        columnMetadata match {
          case Some(meta) if meta.isNullable =>
            s"""   or (tgt.$col is not null and src.$col is null)
               |   or src.$col <> tgt.$col""".stripMargin
          case _ => s"   or src.$col <> tgt.$col"
        }
      })

      val tgtColumns = List.empty
        ::: columnsWithChanges.map(col => s"       coalesce(tgt.$col, src.$col) as $col")
        ::: columnsWithoutChanges.map(col => s"       src.$col as $col")
        ::: uniqueKey.map(col => s"       tgt.$col as $col")
        ::: systemColumns.map(col => s"       ${col._2} as ${col._1}")

      val planTableSql =
        s"""
           |create table ${cache.getLocation}_3 as
           |with src as (select * from $tableSchema.$tableName where valid_to_dttm = to_date('2100','yyyy')),
           |     tgt as (select * from ${cache.getLocation}_2)
           |select case when src.${uniqueKey.head} is not null then 'Update' else 'Insert' end as update_command,
           |       ${tgtColumns.mkString(",\n")}
           |  from tgt
           |  left join src on $joinString
           |where src.${uniqueKey.head} is null
           |${orList.mkString("\n")}
           |""".stripMargin

      runSQL(planTableSql)

      // Update valid interval
      val updateValidIntervalSql =
        s"""
           |update $tableSchema.$tableName tgt
           |   set valid_to_dttm = src.valid_to_dttm - interval '1 microsecond'
           |  from ${cache.getLocation}_3 src
           | where tgt.valid_to_dttm = to_date('2100','yyyy')
           |   and src.update_command = 'Update'
           |   and $joinString
           |""".stripMargin

      runSQL(updateValidIntervalSql)

      val insertNewIntervalSql =
        s"""
           |insert into $tableSchema.$tableName(${(uniqueKey ::: columnsWithChanges ::: columnsWithoutChanges ::: systemColumns.map(col => col._1)).mkString(", ")})
           |select ${(uniqueKey ::: columnsWithChanges ::: columnsWithoutChanges ::: systemColumns.map(col => col._1)).mkString(", ")}
           |  from ${cache.getLocation}_3 src
           |  where src.update_command = 'Insert'
           |""".stripMargin

      runSQL(insertNewIntervalSql)

      logOperationEnd("PostgreSQL merge operation", startTime, s"table: $tableSchema.$tableName")

    } catch {
      case e: Exception =>
        logError("PostgreSQL merge operation", e, s"table: $tableSchema.$tableName")
        throw e
    } finally {
      // Clean up temporary tables
      try {
        runSQL(s"drop table if exists ${cache.getLocation}_3")
        runSQL(s"drop table if exists ${cache.getLocation}_2")
        runSQL(s"drop table if exists ${cache.getLocation}")
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }

  override def writeDf(df: DataFrame, tableSchema: String, tableName: String, writeMode: WriteMode, scdType: SCDType): Unit = {
    val startTime = logOperationStart("PostgreSQL write DataFrame", s"table: ${tableSchema}.${tableName}, mode: $writeMode")
    var locDf: DataFrame = df
    scdType match {
      case SCDType.SCD_3 => {
        locDf = locDf.withColumn("valid_from_dttm", org.apache.spark.sql.functions.current_timestamp())
        locDf = locDf.withColumn("valid_to_dttm", org.apache.spark.sql.functions.to_timestamp(org.apache.spark.sql.functions.lit("2100-01-01 00:00:00")))
        locDf = locDf.withColumn("run_id", org.apache.spark.sql.functions.lit(s"'${ApplicationContext.getRunId}'"))
      }
      case SCDType.SCD_2 => {
        locDf = locDf.withColumn("valid_from_dttm", org.apache.spark.sql.functions.current_timestamp())
        locDf = locDf.withColumn("valid_to_dttm", org.apache.spark.sql.functions.to_timestamp(org.apache.spark.sql.functions.lit("2100-01-01 00:00:00")))
      }
      case SCDType.SCD_0 => {
      }
      case _ => {
        throw NotImplementedException("Method not implemented")
      }
    }
    try {
      logInfo(s"Writing DataFrame to PostgreSQL table: ${tableSchema}.${tableName}")
      locDf.write
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
    println(sql) //TODO
    try {
      stm.execute(sql)
    } catch {
      case e: Exception=> {
        throw e
      }
    }
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
    // Remove from cache when connection is closed
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = {
    ConnectionEnum.postgres
  }

  override def getConfigLocation(): String = {
    _configLocation
  }

  override def encodeDataType(in: TableMetadataType): String = LoaderPostgres.encodeDataType(in)

  override def decodeDataType(in: String): TableMetadataType = LoaderPostgres.decodeDataType(in)

  override def readDf(tableSchema: String, tableName: String): DataFrame = throw Exception("Method not implemented")

  override def readDf(tableSchema: String, tableName: String, partitionName: String): DataFrame = throw Exception("Method not implemented")
}

object LoaderPostgres extends YamlClass {
  def apply(inConfig: String): LoaderPostgres = {
    val configYaml: YamlConfig = mapper.readValue(getLines(inConfig), classOf[YamlConfig])
    return new LoaderPostgres(configYaml, inConfig)
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