package pro.datawiki.sparkLoader.connection.minIo.minioBatch

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.connection.FileStorageTrait
import pro.datawiki.sparkLoader.connection.fileBased.FileBaseFormat
import pro.datawiki.sparkLoader.connection.minIo.minioBase.{LoaderMinIo, YamlConfig}
import pro.datawiki.sparkLoader.dictionaryEnum.WriteMode
import pro.datawiki.sparkLoader.traits.LoggingTrait

class LoaderMinIoBatch(format: FileBaseFormat, configYaml: YamlConfig) extends LoaderMinIo(format, configYaml) with FileStorageTrait with LoggingTrait {

  private def write(df: DataFrame, inFormat:String, inWriteMode:String, finishLocation:String, useCache: Boolean):Unit={
    logInfo(s"Starting write operation - Format: $inFormat, Mode: $inWriteMode, Location: $finishLocation, UseCache: $useCache")
    
    try {
      val dataFrameToWrite = if (useCache) {
        logInfo("Caching DataFrame before write operation")
        df.cache()
      } else {
        df
      }
      
      // Force materialization and ensure completion
      val writer = dataFrameToWrite.write.format(inFormat).mode(inWriteMode)
      
      // Add options for better reliability
      writer
        .option("compression", "snappy")
        .option("fs.s3a.fast.upload", "true")
        .option("fs.s3a.fast.upload.buffer", "disk")
        .option("fs.s3a.committer.name", "directory")
//        .option("fs.s3a.committer.staging.conflict-mode", "replace")//TODO не трогать вызывает проблемы
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
      
    } catch {
      case e: Exception =>
        logError(s"Write operation failed for location: $finishLocation", e)
        throw e
    }
  }

  override def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    writeDf(df, location, writeMode, useCache = true)
  }

  def writeDf(df: DataFrame, location: String, writeMode: WriteMode, useCache: Boolean): Unit = {
    val startTime = logOperationStart("write DataFrame to MinIO", s"location: $location, mode: $writeMode, format: $format, useCache: $useCache")

    try {
      logInfo(s"Writing DataFrame to MinIO: ${getLocation(location)}")

      if (df == null) then throw new IllegalArgumentException("DataFrame cannot be null")

      val optimizedDf = super.optimizeDataFramePartitions(df)
      write(
        optimizedDf,
        format.toString,
        writeMode.toSparkString,
        getLocation(location),
        useCache
      )
      logOperationEnd("write DataFrame to MinIO", startTime, s"location: $location")

    } catch {
      case e: Exception =>
        logError("write DataFrame to MinIO", e, s"location: $location")
        throw e
    }
  }

  @Override
  override def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit = {
    writeDfPartitionDirect(df, location, partitionName, partitionValue, writeMode, useCache = true)
  }

  def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode, useCache: Boolean): Unit = {
    val optimizedDf = super.optimizeDataFramePartitions(df)

    write(
      optimizedDf,
      format.toString,
      writeMode.toSparkString,
      this.getLocation(location, partitionName, partitionValue),
      useCache
    )
  }

  @Override
  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit = {
    val optimizedDf = super.optimizeDataFramePartitions(df)
    optimizedDf.orderBy(partitionName.head, partitionName *).write.format(format.toString).option("nullValue", "\\N").partitionBy(partitionName *).mode(writeMode.toSparkString).save(s"s3a://${configYaml.bucket}/${location}/")
    return

  }


  override def close(): Unit = {
  }
}
