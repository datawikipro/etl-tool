package pro.datawiki.sparkLoader.connection.minIo.minioIceberg

import org.apache.spark.sql.DataFrame
import pro.datawiki.exception.{NotImplementedException, TableNotExistException}
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait, SupportIdMap}
import pro.datawiki.sparkLoader.connection.minIo.minioBase.YamlConfigHost
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.connection.trino.LoaderTrino
import pro.datawiki.sparkLoader.SparkObject

import java.net.Socket

class LoaderMinIoIceberg(val configYaml: YamlConfigIceberg, val configLocation: String)
  extends FileStorageTrait with SupportIdMap with LoggingTrait {


  private val _configLocation: String = configLocation

  logInfo(s"Creating Iceberg connection: catalog=${configYaml.catalog}, warehouse=${configYaml.warehouse}")

  // ─── S3A + Iceberg Spark Configuration ────────────────────────────────────

  def modifySpark(): Unit = {
    val endpoint = getMinIoHost
    val cat = configYaml.catalog

    // 1. Set Iceberg catalog Spark configs BEFORE calling setHadoopConfiguration (which triggers SparkSession creation)
    SparkObject.setConf(s"spark.sql.catalog.$cat", "org.apache.iceberg.spark.SparkCatalog")
    val catType = configYaml.catalogType.getOrElse(
      if (configYaml.hiveMetastoreUri == null || configYaml.hiveMetastoreUri.trim.isEmpty || configYaml.hiveMetastoreUri == "hadoop") "hadoop" else "hive"
    )
    SparkObject.setConf(s"spark.sql.catalog.$cat.type", catType)
    if (catType == "hive") {
      SparkObject.setConf(s"spark.sql.catalog.$cat.uri", configYaml.hiveMetastoreUri)
    }
    SparkObject.setConf(s"spark.sql.catalog.$cat.warehouse", configYaml.warehouse)

    // Iceberg S3 FileIO configs
    SparkObject.setConf(s"spark.sql.catalog.$cat.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    SparkObject.setConf(s"spark.sql.catalog.$cat.s3.endpoint", endpoint)
    SparkObject.setConf(s"spark.sql.catalog.$cat.s3.access-key-id", configYaml.accessKey)
    SparkObject.setConf(s"spark.sql.catalog.$cat.s3.secret-access-key", configYaml.secretKey)
    SparkObject.setConf(s"spark.sql.catalog.$cat.s3.path-style-access",
      configYaml.pathStyleAccess.getOrElse(true).toString)
    SparkObject.setConf(s"spark.sql.catalog.$cat.client.region",
      configYaml.region.getOrElse("us-east-1"))

    // 2. Set S3A credentials and endpoint (this will trigger SparkSession initialization if not already done)
    SparkObject.setHadoopConfiguration("fs.s3a.endpoint", endpoint)
    SparkObject.setHadoopConfiguration("fs.s3a.access.key", configYaml.accessKey)
    SparkObject.setHadoopConfiguration("fs.s3a.secret.key", configYaml.secretKey)
    SparkObject.setHadoopConfiguration("fs.s3a.path.style.access",
      configYaml.pathStyleAccess.getOrElse(true).toString)
    SparkObject.setHadoopConfiguration("fs.s3a.establish.timeout",
      configYaml.establishTimeout.getOrElse("15000"))
    SparkObject.setHadoopConfiguration("fs.s3a.connection.timeout",
      configYaml.connectionTimeout.getOrElse("60000"))
    SparkObject.setHadoopConfiguration("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    SparkObject.setHadoopConfiguration("fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    SparkObject.setHadoopConfiguration("fs.s3a.change.detection.mode", "none")

    logInfo(s"Iceberg catalog '$cat' configured with type '$catType' and metastore URI '${configYaml.hiveMetastoreUri}'")
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  /** Resolves active MinIO/S3 host from the failover list. */
  private def getMinIoHost: String = {
    configYaml.minioHost.foreach { host =>
      val socket = new Socket()
      try {
        socket.connect(new java.net.InetSocketAddress(host.hostName, host.hostPort),
          configYaml.establishTimeout.getOrElse("30000").toInt)
        return host.getUrl
      } catch {
        case _: Exception => // try next host
      } finally {
        if (socket != null) socket.close()
      }
    }
    throw NotImplementedException("No reachable MinIO host found in Iceberg config")
  }

  private def parseLocation(location: String): (String, String) = {
    if (location.contains('/')) {
      val lastSlashIdx = location.lastIndexOf('/')
      val pathBefore = location.substring(0, lastSlashIdx)
      val tableName = location.substring(lastSlashIdx + 1)
      val schemaName = if (pathBefore.contains('/')) {
        pathBefore.substring(pathBefore.lastIndexOf('/') + 1)
      } else {
        pathBefore
      }
      (schemaName, tableName)
    } else {
      val lastDotIdx = location.lastIndexOf('.')
      if (lastDotIdx != -1) {
        val schemaName = location.substring(0, lastDotIdx)
        val tableName = location.substring(lastDotIdx + 1)
        (schemaName, tableName)
      } else {
        ("default", location)
      }
    }
  }

  /** Full Iceberg table reference: catalog.schema.table */
  private def fullRef(location: String): String = {
    val (schemaName, tableName) = parseLocation(location)
    if (schemaName != "default") {
      s"${configYaml.catalog}.`$schemaName`.$tableName"
    } else {
      s"${configYaml.catalog}.$location"
    }
  }

  /** Ensures the Iceberg schema (Hive database) exists, with S3 location at {schema}.db/. */
  private def createSchemaIfNotExists(tableRef: String): Unit = {
    val (schemaName, _) = parseLocation(tableRef)
    if (schemaName != "default") {
      val schemaRef = s"${configYaml.catalog}.`$schemaName`"
      // Physical S3 location always uses the .db suffix convention
      val s3SchemaFolder = if (schemaName.endsWith(".db")) schemaName else schemaName + ".db"
      val schemaLocation = s"${configYaml.warehouse}/$s3SchemaFolder"
      logInfo(s"Ensuring Iceberg schema exists: $schemaRef LOCATION '$schemaLocation'")
      try {
        SparkObject.spark.sql(s"CREATE DATABASE IF NOT EXISTS $schemaRef LOCATION '$schemaLocation'")
      } catch {
        case e: Exception =>
          logWarning(s"Could not create database $schemaRef with LOCATION: ${e.getMessage}. Retrying without LOCATION...")
          try {
            SparkObject.spark.sql(s"CREATE DATABASE IF NOT EXISTS $schemaRef")
          } catch {
            case ex: Exception =>
              logWarning(s"Could not create database $schemaRef without LOCATION: ${ex.getMessage}. Proceeding anyway...")
          }
      }
    }
  }

  // ─── Trino JDBC Registry ──────────────────────────────────────────────────

  /** Parses the register config block to instantiate a LoaderTrino if configured */
  def getTrinoLoader: Option[LoaderTrino] = {
    configYaml.register.flatMap { cfg =>
      cfg.registerType.toLowerCase match {
        case "trino-jdbc" =>
          val url = cfg.url.getOrElse("")
          val user = cfg.user.getOrElse("chernousov_a")
          if (url.nonEmpty) {
            Some(new LoaderTrino(url, user))
          } else {
            logWarning("Trino JDBC registry URL is empty, skipping registration")
            None
          }
        case other =>
          logWarning(s"Unknown register type: $other")
          None
      }
    }
  }

  // ─── Write ────────────────────────────────────────────────────────────────

  /**
   * Writes DataFrame to an Iceberg table using createOrReplace (full table overwrite).
   * `location` is expected in format "schema.table"
   * e.g. "ods__ozon.my_table"
   */
  def writeDf(df: DataFrame, tableName: String, location: String, writeMode: WriteMode, partitionName: List[String]): Unit = {
    modifySpark()
    val ref = fullRef(location)
    val startTime = logOperationStart("write Iceberg table", s"ref: $ref, mode: $writeMode")
    try {
      createSchemaIfNotExists(location)

      val exists = SparkObject.spark.catalog.tableExists(ref)

      if (!exists) {
        logInfo(s"Table $ref does not exist. Creating and initializing it.")
        var writer = df.writeTo(ref)
          .tableProperty("format-version", "2")
          .tableProperty("write.format.default", "parquet")

        if (partitionName.nonEmpty) {
          import org.apache.spark.sql.functions.col
          writer = writer.partitionedBy(col(partitionName.head), partitionName.tail.map(col)*)
        }
        writer.create()
      } else {
        writeMode match {
          case WriteMode.overwritePartition =>
            logInfo(s"Writing to Iceberg table: $ref (overwritePartitions - dynamic partition overwrite)")
            df.writeTo(ref)
              .overwritePartitions()
          case _ =>
            logInfo(s"Writing to Iceberg table: $ref (createOrReplace - full table overwrite)")
            df.writeTo(ref)
              .tableProperty("format-version", "2")
              .tableProperty("write.format.default", "parquet")
              .createOrReplace()
        }
      }
      logOperationEnd("write Iceberg table", startTime, s"ref: $ref")

      getTrinoLoader.foreach { trino =>
        val (locSchemaName, locTableName) = parseLocation(location)
        if (locSchemaName != "default") {
          // Physical S3 path uses .db suffix from locSchemaName
          val s3SchemaFolder = if (locSchemaName.endsWith(".db")) locSchemaName else locSchemaName + ".db"
          val tableLocation = s"${configYaml.warehouse}/$s3SchemaFolder/$locTableName"
          
          // Parse Trino/logical table from tableName parameter (tableName in YAML)
          val logicalLastDotIdx = tableName.lastIndexOf('.')
          val (trinoSchema, trinoTable) = if (logicalLastDotIdx != -1) {
            (tableName.substring(0, logicalLastDotIdx), tableName.substring(logicalLastDotIdx + 1))
          } else {
            (locSchemaName, locTableName)
          }
          
          trino.registerTable(configYaml.catalog, trinoSchema, trinoTable, tableLocation)
        }
      }
    } catch {
      case e: Exception =>
        logError("write Iceberg table", e, s"ref: $ref")
        throw e
    }
  }

  override def writeDf(df: DataFrame, tableName: String, location: String, writeMode: WriteMode): Unit = {
    writeDf(df, tableName, location, writeMode, List.empty)
  }

  override def writeDfPartitionAuto(df: DataFrame, tableName: String, location: String,
                                    partitionName: List[String], writeMode: WriteMode): Unit = {
    writeDf(df, tableName, location, writeMode, partitionName)
  }

  override def writeDfPartitionDirect(df: DataFrame, tableName: String, location: String,
                                      partitionName: List[String], partitionValue: List[String],
                                      writeMode: WriteMode, useCache: Boolean): Unit = {
    writeDf(df, tableName, location, writeMode, partitionName)
  }

  // ─── Read ─────────────────────────────────────────────────────────────────

  override def readDf(location: String): DataFrame = {
    modifySpark()
    createSchemaIfNotExists(location)
    val ref = fullRef(location)
    logInfo(s"Reading Iceberg table: $ref")
    SparkObject.spark.read.format("iceberg").load(ref)
  }

  override def readDf(location: String, keyPartitions: List[String],
                      valuePartitions: List[String], withPartitionOnDataframe: Boolean): DataFrame = {
    import org.apache.spark.sql.functions.lit
    var df = readDf(location)
    val filter = keyPartitions.zip(valuePartitions)
      .map { case (k, v) => s"$k = '$v'" }
      .mkString(" AND ")
    if (filter.nonEmpty) df = df.where(filter)
    if (withPartitionOnDataframe) {
      keyPartitions.zip(valuePartitions).foreach { case (col, value) =>
        df = df.withColumn(col, lit(value))
      }
    }
    df
  }

  override def readDfSchema(location: String): DataFrame = {
    modifySpark()
    createSchemaIfNotExists(location)
    SparkObject.spark.read.format("iceberg").load(fullRef(location)).limit(0)
  }

  // ─── Unsupported operations ───────────────────────────────────────────────

  override def saveRaw(in: String, inLocation: String): Unit =
    throw NotImplementedException("saveRaw not supported for Iceberg")

  override def moveTablePartition(oldTable: String, newTable: String,
                                  partitionName: List[String]): Boolean =
    throw NotImplementedException("moveTablePartition not supported for Iceberg")

  override def getMasterFolder: String = configYaml.warehouse

  override def getFolder(location: String): List[String] =
    throw NotImplementedException("getFolder not supported for Iceberg")

  override def deleteFolder(location: String): Boolean =
    throw NotImplementedException("deleteFolder not supported for Iceberg")

  override def close(): Unit = {
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = ConnectionEnum.minioIceberg

  override def getConfigLocation(): String = _configLocation

  // Registration logic is handled via LoaderTrino using getTrinoLoader

  // ─── SupportIdMap Implementation ──────────────────────────────────────────

  override def createViewIdMapGenerate(tableName: String, surrogateKey: List[String]): String = {
    val targetTableName = "tmp_idmap_gen_" + scala.util.Random.alphanumeric.filter(_.isLetter).take(10).mkString
    val sql =
      s"""CREATE OR REPLACE TEMPORARY VIEW $$targetTableName AS
         |SELECT concat_ws('!@#', $${surrogateKey.mkString(", ")}) as ccd
         |  FROM $${configYaml.catalog}.$${tableName}
         | WHERE coalesce($${surrogateKey.mkString(", ")}) is not null
         |   AND concat_ws('!@#', $${surrogateKey.mkString(", ")}) != ''
         | GROUP BY 1
         |""".stripMargin
    SparkObject.spark.sql(sql)
    targetTableName
  }

  override def createViewIdMapMerge(tableName: String, inSurrogateKey: List[String], outSurrogateKey: List[String]): String = {
    val targetTableName = "tmp_idmap_merge_" + scala.util.Random.alphanumeric.filter(_.isLetter).take(10).mkString
    val sql =
      s"""CREATE OR REPLACE TEMPORARY VIEW $$targetTableName AS
         |SELECT concat_ws('!@#', $${inSurrogateKey.mkString(", ")}) as in_ccd,
         |       concat_ws('!@#', $${outSurrogateKey.mkString(", ")}) as out_ccd
         |  FROM $${configYaml.catalog}.$${tableName}
         | WHERE coalesce($${inSurrogateKey.mkString(", ")}) is not null
         |   AND coalesce($${outSurrogateKey.mkString(", ")}) is not null
         | GROUP BY 1, 2
         |""".stripMargin
    SparkObject.spark.sql(sql)
    targetTableName
  }

  override def generateIdMap(inTable: String, domain: String, systemCode: String): Boolean = {
    val targetTable = s"${configYaml.catalog}.idmap.$domain"
    val sql =
      s"""INSERT INTO $targetTable (ccd, source_code, rk)
         |WITH max_rk AS (
         |    SELECT coalesce(max(rk), 0) AS max_val FROM $targetTable
         |),
         |new_rows AS (
         |    SELECT data.ccd, '$systemCode' as source_code
         |      FROM $inTable as data
         |      LEFT JOIN $targetTable id ON id.ccd = data.ccd AND id.source_code = '$systemCode'
         |     WHERE id.ccd IS NULL
         |)
         |SELECT new_rows.ccd, new_rows.source_code, max_val + ROW_NUMBER() OVER(ORDER BY new_rows.ccd) as rk
         |  FROM new_rows CROSS JOIN max_rk
         |""".stripMargin
    SparkObject.spark.sql(sql)
    true
  }

  override def mergeIdMap(inTable: String, domain: String, inSystemCode: String, outSystemCode: String): Boolean = {
    val targetTable = s"${configYaml.catalog}.idmap.$domain"
    val sql =
      s"""INSERT INTO $targetTable (ccd, source_code, rk)
         |WITH in_idmap AS (SELECT ccd, rk FROM $targetTable WHERE source_code = '$inSystemCode'),
         |     out_idmap AS (SELECT ccd FROM $targetTable WHERE source_code = '$outSystemCode')
         |SELECT data.out_ccd, '$outSystemCode', MAX(in_idmap.rk)
         |  FROM $inTable as data
         |  JOIN in_idmap ON in_ccd = in_idmap.ccd
         |  LEFT JOIN out_idmap ON out_ccd = out_idmap.ccd
         | WHERE out_idmap.ccd IS NULL
         | GROUP BY data.out_ccd
         |HAVING count(distinct in_idmap.rk) = 1
         |""".stripMargin
    SparkObject.spark.sql(sql)
    true
  }
}

object LoaderMinIoIceberg extends pro.datawiki.yamlConfiguration.YamlClass {
  def apply(inConfig: String): LoaderMinIoIceberg = {
    val configYaml: YamlConfigIceberg = mapper.readValue(getLines(inConfig), classOf[YamlConfigIceberg])
    val loader = new LoaderMinIoIceberg(configYaml, inConfig)
    loader.modifySpark()
    loader
  }
}

