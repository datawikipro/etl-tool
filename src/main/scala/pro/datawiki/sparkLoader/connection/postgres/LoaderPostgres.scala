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
import java.util.concurrent.ConcurrentHashMap
import scala.util.Random

class LoaderPostgres(configYaml: YamlConfig, configLocation: String) extends ConnectionTrait, DatabaseTrait, SupportIdMap, LoggingTrait {
  var schemaName: String = ""

  private def defaultSchema: String = {
    if schemaName != "" then return schemaName
    schemaName = s"tmp__${Random.alphanumeric.filter(_.isLetter).take(16).mkString}"
    runSQL(s"create schema $schemaName;")
    return schemaName
  }

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

  override def writeDfAppend(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, targetColumns: List[String], uniqueKey: List[String], partitionBy: List[(String,String)]): Unit = {
    val table = TableSqlGenerate(tableSchema, tableName, scdType, targetColumns, uniqueKey, List.empty, this)
    
    uniqueKey.length match {
      case 0 => super.writeDfAppend(df, tableSchema, tableName, scdType, targetColumns, uniqueKey,partitionBy)
      case _ => {
        val cache: TransformationCacheDatabase = TransformationCacheDatabase()
        cache.saveTable(DataFrameOriginal(df), overwriteTable, this)
        try {
          runSQL(
            s"""insert into ${table.getTableSchema}.${table.getTableName}(${table.getColumnKey.mkString(",")})
               |select ${table.getColumnValue.mkString(",")} from ${cache.getLocation}
               | ON CONFLICT (${table.getUniqueKey.mkString(",")}) DO NOTHING
               |""".stripMargin)
        } catch {
          case e: Exception => {
            throw e
          }
        } finally {
          runSQL(s"""drop table if exists ${cache.getLocation}""".stripMargin)
        }
      }
    }

  }
  
  override def writeDfMergeFull(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, columns: List[String], uniqueKey: List[String], partitionBy: List[(String,String)]): Unit = {
    writeDfMergeDelta(df, tableSchema, tableName, scdType, columns, uniqueKey,partitionBy)
    val cache: TransformationCacheDatabase = TransformationCacheDatabase()
    cache.saveTable(DataFrameOriginal(df), WriteMode.overwriteTable, this)
    val deltaTableSql =
      s"""
         |select ${(uniqueKey).map(col => s""""${col}"""").mkString(",")} from $tableSchema.$tableName
         |where valid_to_dttm = to_date('2100','yyyy')
         |except
         | select ${uniqueKey.map(col => s""""${col}"""").mkString(",")} from ${cache.getLocation} where ${uniqueKey.head} is not null
         |""".stripMargin
    val createDeltaTableSql = s"create table ${cache.getLocation}_4 as $deltaTableSql"
    runSQL(createDeltaTableSql)

    val updateValidIntervalSql =
      s"""
         |update $tableSchema.$tableName tgt
         |   set valid_to_dttm = now() - interval '1 microsecond'
         |  from ${cache.getLocation}_4 src
         | where tgt.valid_to_dttm = to_date('2100','yyyy')
         |   and ${uniqueKey.map(col => s"""src."${col}" = tgt."${col}"""").mkString(" and ")}
         |""".stripMargin

    runSQL(updateValidIntervalSql)

    val insertDeletedSql = {
      s"""
         |insert into $tableSchema.$tableName (${uniqueKey.map(col => s""""${col}"""").mkString(",")})
         |select ${uniqueKey.map(col => s""""${col}"""").mkString(",")}
         |  from ${cache.getLocation}_4
         |""".stripMargin
    }

    runSQL(insertDeletedSql)
  }

  override def writeDfMergeDelta(df: DataFrame, tableSchema: String, tableName: String, scdType: SCDType, columns: List[String], uniqueKey: List[String], partitionBy: List[(String,String)]): Unit = {
    val startTime = logOperationStart("PostgreSQL merge operation", s"table: $tableSchema.$tableName")
    val table = TableSqlGenerate(tableSchema, tableName, scdType, columns, uniqueKey,partitionBy, this)
    if !table.initTableAlias(defaultSchema) then throw Exception()
    // Create cache for temporary table
    val cache: TransformationCacheDatabase = TransformationCacheDatabase()

    try {
      if (uniqueKey.isEmpty) {
        throw new IllegalArgumentException("Unique key is required for merge operation")
      }

      logInfo(s"Starting merge operation for PostgreSQL table: $tableSchema.$tableName for partition ${partitionBy.map(col => s"${col._1 -> col._2}").mkString(", ")}")

      logInfo(s"Starting merge step1: $tableSchema.$tableName for partition ${partitionBy.map(col => s"${col._1 -> col._2}").mkString(", ")}")

      cache.saveTable(DataFrameOriginal(df), WriteMode.overwriteTable, this)


      val deltaTableSql =
        s"""
           |select ${(uniqueKey ::: table.getColumnsWithChanges).map(col => s""""${col}"""").mkString(",")} from ${cache.getLocation} where ${uniqueKey.head} is not null
           |except
           |select ${(uniqueKey ::: table.getColumnsWithChanges).map(col => s""""${col}"""").mkString(",")} from ${table.getTableAlias}
           |""".stripMargin


      val createDeltaTableSql = s"create table ${cache.getLocation}_2 as $deltaTableSql"

      logInfo(s"Starting merge step2: $tableSchema.$tableName for partition ${partitionBy.map(col => s"${col._1 -> col._2}").mkString(", ")}")

      runSQL(createDeltaTableSql)

      // Calculate plan table
      val joinString = uniqueKey.map(col => s"""src."${col}" = tgt."${col}"""").mkString(" and ")

      val orList = table.getColumnsWithChanges.map(col => {
        val columnMetadata: Option[TableMetadataColumn] = getTableColumns(tableSchema, tableName).find(_.column_name == col)
        var result = s"""   or src."${col}" <> tgt."${col}""""
        if (columnMetadata.isDefined) {
          if columnMetadata.get.isNullable then
            result += s"""   or (tgt."${col}" is not null and src."${col}" is null)""".stripMargin
        }
        result
      })

      val tgtColumns = List.empty
        ::: table.getColumnsWithChanges.map(col => s"""       coalesce(tgt."${col}", src."${col}") as "${col}"""")
        ::: table.getColumnsWithoutChanges.map(col => s"""       src."${col}" as "${col}"""")
        ::: uniqueKey.map(col => s"""       tgt."${col}" as "${col}"""")
        ::: table.getSystemColumnValue.map(col => s"""       ${col}""")

      val planTableSql =
        s"""
           |create table ${cache.getLocation}_3 as
           |with src as (select * from ${table.getTableAlias}),
           |     tgt as (select * from ${cache.getLocation}_2)
           |select case when src.${uniqueKey.head} is not null then 'Update' else 'Insert' end as update_command,
           |       ${tgtColumns.mkString(",\n")}
           |  from tgt
           |  left join src on $joinString
           |where src.${uniqueKey.head} is null
           |${orList.mkString("\n")}
           |""".stripMargin
      logInfo(s"Starting merge step3: $tableSchema.$tableName for partition ${partitionBy.map(col => s"${col._1 -> col._2}").mkString(", ")}")

      runSQL(planTableSql)

      // Update valid interval
      val updateValidIntervalSql =
        s"""
           |update ${table.getTableAlias} tgt
           |   set valid_to_dttm = src.valid_from_dttm - interval '1 microsecond'
           |  from ${cache.getLocation}_3 src
           | where tgt.valid_to_dttm = to_date('2100','yyyy')
           |   and src.update_command = 'Update'
           |   and $joinString
           |""".stripMargin

      runSQL(updateValidIntervalSql)
      logInfo(s"Starting merge step5: $tableSchema.$tableName for partition ${partitionBy.map(col => s"${col._1 -> col._2}").mkString(", ")}")

      val insertNewIntervalSql =
        s"""
           |insert into ${table.getTableAlias}(${((uniqueKey ::: table.getColumnsWithChanges ::: table.getColumnsWithoutChanges).map(col => s""""${col}"""") ::: table.getSystemColumnKey).mkString(", ")})
           |select ${((uniqueKey ::: table.getColumnsWithChanges ::: table.getColumnsWithoutChanges).map(col => s""""${col}"""") ::: table.getSystemColumnValue).mkString(", ")}
           |  from ${cache.getLocation}_3 src
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

  override def writeDf(df: DataFrame, tableSchema: String, tableName: String, writeMode: WriteMode, scdType: SCDType, partitionBy: List[(String,String)]): Unit = {
    val startTime = logOperationStart("PostgreSQL write DataFrame", s"table: ${tableSchema}.${tableName}, mode: $writeMode")
    var locDf: DataFrame = df
    scdType match {
      case SCDType.SCD_3 => {
        locDf = locDf.withColumn("valid_from_dttm", org.apache.spark.sql.functions.current_timestamp())
        locDf = locDf.withColumn("valid_to_dttm", org.apache.spark.sql.functions.to_timestamp(org.apache.spark.sql.functions.lit("2100-01-01 00:00:00")))
        locDf = locDf.withColumn("run_id", org.apache.spark.sql.functions.lit(s"${ApplicationContext.getRunId}"))
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
    val sql =
      s"""insert into idmap.${domain}(ccd, system_code)
         |with id as (
         |    select ccd
         |      from idmap.${domain}
         |     where system_code = '${systemCode}')
         |select data.ccd,'${systemCode}'
         |  from $defaultSchema.${inTable} as data
         |  left join id using (ccd)
         | where id.ccd is null
         |   """.stripMargin
    runSQL(sql)
  }

  override def mergeIdMap(inTable: String, domain: String, inSystemCode: String, outSystemCode: String): Boolean = {
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
    runSQL(sql)
  }

  override def readDfSchema(tableSchema: String, tableName: String): DataFrame = {
    val df = SparkObject.spark.sqlContext.read
      .option("fetchsize", "10000")
      .jdbc(getJdbc, s"""(select * from ${tableSchema}.${tableName} where 1 = 2) a """, getProperties)
    LogMode.debugDF(df)
    return df
  }

  override def runSQL(sql: String): Boolean = {
    val stm = getConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    try {
      stm.execute(sql)
    } catch {
      case e: Exception => {
        println(sql) //TODO
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

  // Cache for table column metadata to avoid repeated database queries
  private val tableColumnsCache: ConcurrentHashMap[String, List[TableMetadataColumn]] = new ConcurrentHashMap[String, List[TableMetadataColumn]]()

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
    return DwhConnectionInfo(
      host = server.host,
      port = server.port,
      database = server.database,
      username = configYaml.login,
      password = configYaml.password
    )
  }

  def getTableColumns(tableSchema: String, tableName: String): List[TableMetadataColumn] = {
    val cacheKey = s"$tableSchema.$tableName"

    // Check cache first
    tableColumnsCache.get(cacheKey) match {
      case cachedColumns if cachedColumns != null =>
        logInfo(s"Using cached table columns for $cacheKey")
        return cachedColumns
      case _ => // Continue with database query
    }

    logInfo(s"Fetching table columns from database for $cacheKey")
    val sql =
      s"""select column_name,
         |       data_type,
         |       is_nullable
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
        row.get(2).toString match {
          case "YES" => true
          case "NO" => false
          case fs => throw Exception()
        }
      ))

      // Cache the result
      tableColumnsCache.put(cacheKey, b)
      logInfo(s"Cached table columns for $cacheKey")

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
    if schemaName != "" then {
      runSQL(s"drop schema $defaultSchema cascade;")
    }
    connection.close()
    // Clear table columns cache when connection is closed
    tableColumnsCache.clear()
    logInfo("Cleared table columns cache")
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

  override def setTemporaryTable(tableName: String, sql: String): Boolean = {
    runSQL(s"create view ${defaultSchema}.$tableName as $sql")
  }

  override def createViewIdMapGenerate(tableName: String, surrogateKey: List[String]): String = {
    val targetTableName = Random.alphanumeric.filter(_.isLetter).take(16).mkString
    val sql =
      s"""
         |select ${surrogateKey.mkString("|| '!@#' ||")} as ccd
         |  from $defaultSchema.${tableName}
         | where nullif(coalesce(${surrogateKey.mkString(",")}),'') is not null
         | group by ccd
         |  """.stripMargin

    setTemporaryTable(targetTableName, sql)
    return targetTableName
  }

  override def createViewIdMapMerge(tableName: String, inSurrogateKey: List[String], outSurrogateKey: List[String]): String = {
    val targetTableName = Random.alphanumeric.filter(_.isLetter).take(16).mkString
    val sql =
      s"""
         |select ${inSurrogateKey.mkString("!@#")}  as in_ccd,
         |       ${outSurrogateKey.mkString("!@#")} as out_ccd
         |  from $defaultSchema.$tableName
         | where coalesce(${inSurrogateKey.mkString(",")}) is not null
         |   and coalesce(${outSurrogateKey.mkString(",")}) is not null
         | group by in_ccd, out_ccd
         |  """.stripMargin

    setTemporaryTable(targetTableName, sql)
    return targetTableName


  }
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