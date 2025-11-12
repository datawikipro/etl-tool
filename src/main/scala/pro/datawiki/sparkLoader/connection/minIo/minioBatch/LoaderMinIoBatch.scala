package pro.datawiki.sparkLoader.connection.minIo.minioBatch

import org.apache.spark.sql.{DataFrame, SparkSession}
import pro.datawiki.sparkLoader.connection.FileStorageTrait
import pro.datawiki.sparkLoader.connection.fileBased.FileBaseFormat
import pro.datawiki.sparkLoader.connection.minIo.minioBase.{LoaderMinIo, YamlConfig}
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait

class LoaderMinIoBatch(format: FileBaseFormat, configYaml: YamlConfig, configLocation: String, useHiveMetastore: Boolean = false) extends LoaderMinIo(format, configYaml, configLocation) with FileStorageTrait with LoggingTrait {

  override def getConnectionEnum(): ConnectionEnum = {
    ConnectionEnum.minioParquet
  }

  private def write(df: DataFrame, tableName: String, inFormat: String, inWriteMode: String, finishLocation: String, useCache: Boolean, partitionColumns: List[String]): Unit = {
    logInfo(s"Starting write operation - Format: $inFormat, Mode: $inWriteMode, Location: $finishLocation, UseCache: $useCache, TableName: $tableName, PartitionColumns: $partitionColumns")

    try {
      val dataFrameToWrite = if (useCache) {
        logInfo("Caching DataFrame before write operation")
        df.cache()
      } else {
        df
      }

      var writer = dataFrameToWrite.write.format(inFormat).mode(inWriteMode)

      if partitionColumns.nonEmpty then {
        writer = writer.partitionBy(partitionColumns: _*)
      }

      // Add options for better reliability
      writer
        .option("compression", "snappy")
        .option("fs.s3a.fast.upload", "true")
        .option("fs.s3a.fast.upload.buffer", "disk")
        .option("fs.s3a.committer.name", "directory")
        .option("fs.s3a.committer.staging.unique-filenames", "true")

      logInfo(s"Executing write operation to: $finishLocation")

      // Execute write and force completion
      writer.save(finishLocation)

      // Force Spark to complete all pending operations and wait for completion
      dataFrameToWrite.sparkSession.sparkContext.statusTracker.getExecutorInfos

      // Additional wait to ensure all operations are completed
      Thread.sleep(1000)

      // Unpersist cached DataFrame if it was cached
      if (useCache) {
        logInfo("Unpersisting cached DataFrame after write operation")
        dataFrameToWrite.unpersist()
      }

      logInfo(s"Write operation completed successfully for location: $finishLocation")

      if (useHiveMetastore && tableName != "") {
        registerTableInHive(dataFrameToWrite.sparkSession, tableName, finishLocation, inFormat, partitionColumns)
      }

    } catch {
      case e: Exception =>
        logError(s"Write operation failed for location: $finishLocation", e)
        throw e
    }
  }

  override def writeDf(df: DataFrame, tableName: String, location: String, writeMode: WriteMode): Unit = {
    writeDf(df = df, tableName = tableName, location = location, writeMode = writeMode, useCache = true, partitionColumns = List.empty)
  }

  def writeDf(df: DataFrame, tableName: String, location: String, writeMode: WriteMode, useCache: Boolean, partitionColumns: List[String]): Unit = {
    val startTime = logOperationStart("write DataFrame to MinIO", s"location: $location, mode: $writeMode, format: $format, useCache: $useCache, tableName: $tableName, partitionColumns: $partitionColumns")

    try {
      logInfo(s"Writing DataFrame to MinIO: ${getLocation(location)}")

      if (df == null) then throw new IllegalArgumentException("DataFrame cannot be null")

      val optimizedDf = super.optimizeDataFramePartitions(df)
      write(
        df= optimizedDf,
        tableName=tableName,
        inFormat=format.toString,
        inWriteMode=writeMode.toSparkString,
        finishLocation=getLocation(location),
        useCache= useCache,
        partitionColumns= partitionColumns
      )
      logOperationEnd("write DataFrame to MinIO", startTime, s"location: $location")

    } catch {
      case e: Exception =>
        logError("write DataFrame to MinIO", e, s"location: $location")
        throw e
    }
  }

  @Override
  override def writeDfPartitionDirect(df: DataFrame, tableName: String, location: String, partitionName: List[String],
                                      partitionValue: List[String], writeMode: WriteMode,useCache:Boolean): Unit = {
    val optimizedDf = super.optimizeDataFramePartitions(df)

    write(
      optimizedDf, 
      tableName,
      format.toString,
      writeMode.toSparkString,
      this.getLocation(location, partitionName, partitionValue),
      useCache,
      partitionName // Assuming partitionName here are the partition columns
    )
  }

  override def writeDfPartitionAuto(df: DataFrame, tableName: String, location: String, partitionName: List[String], writeMode: WriteMode): Unit = {
    val optimizedDf = super.optimizeDataFramePartitions(df.distinct())
    write(
      optimizedDf.orderBy(partitionName.head, partitionName: _*),
      tableName = tableName,
      format.toString,
      writeMode.toSparkString,
      s"s3a://${configYaml.bucket}/${location}/",
      useCache = true, // Assuming caching is desired for auto-partitioned writes
      partitionColumns = partitionName
    )
  }

  private def registerTableInHive(spark: SparkSession, tableName: String, location: String, format: String, partitionColumns: List[String]): Unit = {
    logInfo(s"Attempting to register table '$tableName' in Hive Metastore at location '$location' with format '$format', partitioned by $partitionColumns")
    try {
      val partitionClause = if (partitionColumns.nonEmpty) {
        s"PARTITIONED BY (${partitionColumns.mkString(", ")})"
      } else {
        ""
      }
      spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING $format $partitionClause LOCATION '$location'")
      logInfo(s"Table '$tableName' registered successfully in Hive Metastore.")
    } catch {
      case e: Exception =>
        logError(s"Failed to register table '$tableName' in Hive Metastore.", e)
        throw e
    }
  }

  override def close(): Unit = {
  }
}
