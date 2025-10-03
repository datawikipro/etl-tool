package pro.datawiki.sparkLoader.connection.minIo.minioBase

import _root_.org.apache.spark.sql.functions.lit
import _root_.org.apache.spark.sql.{DataFrame, DataFrameReader}
import io.minio.*
import pro.datawiki.exception.{NotImplementedException, TableNotExistException}
import pro.datawiki.sparkLoader.connection.fileBased.FileBaseFormat
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, FileStorageTrait}
import pro.datawiki.sparkLoader.dictionaryEnum.{ConnectionEnum, WriteMode}
import pro.datawiki.sparkLoader.traits.LoggingTrait
import pro.datawiki.sparkLoader.{LogMode, SparkObject}

import java.io.File
import java.net.URLEncoder.*
import java.net.{InetSocketAddress, Socket}
import scala.jdk.CollectionConverters.*
import scala.util.Random

case class LoaderMinIo(format: FileBaseFormat,
                       configYaml: YamlConfig, 
                       configLocation: String) extends FileStorageTrait, ConnectionTrait, LoggingTrait {
  private val _configLocation: String = configLocation
  
  logInfo("Creating MinIO connection")

  def optimizeDataFramePartitions(df: DataFrame): DataFrame = {
    try {
      val rowCount = df.count()
      val currentPartitions = df.rdd.getNumPartitions

      val targetPartitions = rowCount match {
        case count if count < 200 => 1 // Very small datasets: single file
        case count if count < 500 => 2 // Small datasets: 2 files
        case count if count < 1000 => 4 // Medium-small: 4 files (~250 rows each)
        case count if count < 5000 => Math.max(4, count / 500).toInt // Medium: ~500 rows per partition
        case count if count < 50000 => Math.max(8, count / 1000).toInt // Large: ~1000 rows per partition
        case _ => Math.max(16, Math.min(currentPartitions, 50)) // Very large: cap at 50 partitions
      }

      if (targetPartitions != currentPartitions) {
        logInfo(s"Optimizing partitions: $currentPartitions -> $targetPartitions for $rowCount rows")
        if (targetPartitions < currentPartitions) {
          return df.coalesce(targetPartitions)
        } else {
          return df.repartition(targetPartitions)
        }
      } else {
        logInfo(s"Partition count already optimal: $currentPartitions partitions for $rowCount rows")
        return df
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to optimize partitions, using original DataFrame: ${e.getMessage}")
        return df
    }
  }

  private def createDataFrameReader(): DataFrameReader = {
    logInfo(s"Creating DataFrameReader for format: ${format.toString}")
    var reader = SparkObject.spark.read.format(format.toString)

    // Configure format-specific options
    format match {
      case FileBaseFormat.`json` =>
        logInfo("Configuring DataFrameReader for JSON format")
        if configYaml.corruptRecordColumn.nonEmpty then
          reader = reader.option("columnNameOfCorruptRecord", configYaml.corruptRecordColumn.get)
        if configYaml.jsonMode.nonEmpty then
          reader = reader.option("mode", configYaml.jsonMode.get)
        if configYaml.jsonMultiline.nonEmpty then
          reader = reader.option("multiline", configYaml.jsonMultiline.get.toString)
        return reader
      case FileBaseFormat.`parquet` =>
        logInfo("Configuring DataFrameReader for Parquet format")
        // Parquet doesn't need corrupt record handling, but we can add other options if needed
        reader = reader
          .option("mergeSchema", "true") // Enable schema merging for Parquet files
        return reader
      case FileBaseFormat.`csv` =>
        logInfo("Configuring DataFrameReader for CSV format")
        reader = reader
          .option("header", "true")
          .option("inferSchema", "true")
        return reader
      case _ =>
        logInfo(s"Using default configuration for format: ${format.toString}")
        // For other formats, use default options
        return reader
    }
  }

  def readDf(location: String): DataFrame = {
    val fullLocation = getLocation(location = location)
    logInfo(s"Reading DataFrame from MinIO location: $fullLocation")

    val df: DataFrame = createDataFrameReader().load(fullLocation)
    logInfo(s"Successfully read DataFrame from: $fullLocation")
    return df

  }

  def readDf(location: String, keyPartitions: List[String], valuePartitions: List[String]): DataFrame = {
    val fullLocation = getLocation(location = location, keyPartitions = keyPartitions, valuePartitions = valuePartitions)
    val partitionPath = getLocationWithPostfix(location, keyPartitions, valuePartitions)

    logInfo(s"Reading DataFrame from MinIO with partitions: $fullLocation")
    logInfo(s"Partition path: $partitionPath, keyPartitions: ${keyPartitions.mkString(",")}, valuePartitions: ${valuePartitions.mkString(",")}")
    logInfo(s"Using format: ${format.toString} for reading")

    val list = getListElementsInFolder(configYaml.bucket, partitionPath)
    if list.isEmpty then {
      logWarning(s"No files found in partition path: $partitionPath")
      throw TableNotExistException(s"No files found in MinIO partition path: $partitionPath")
    }

    logInfo(s"Found ${list.size} files in partition: ${list.take(5).mkString(", ")}${if (list.size > 5) "..." else ""}")


    var df: DataFrame = createDataFrameReader().load(fullLocation)

    keyPartitions.zipWithIndex.foreach { case (value, index) =>
      df = df.withColumn(keyPartitions(index), lit(valuePartitions(index)))
    }

    logInfo(s"Successfully read DataFrame with partitions from: $fullLocation")
    LogMode.debugDF(df)
    return df

  }


  def writeDf(df: DataFrame, location: String, writeMode: WriteMode): Unit = {
    throw NotImplementedException("writeDf method not implemented for MinIO")
  }


  def writeDfPartitionDirect(df: DataFrame, location: String, partitionName: List[String], partitionValue: List[String], writeMode: WriteMode): Unit = {
    throw NotImplementedException("writeDfPartitionDirect method not implemented for MinIO")
  }


  val minioClient: MinioClient = MinioClient.builder()
    .endpoint(getMinIoHost)
    .credentials(configYaml.accessKey, configYaml.secretKey)
    .build()

  def saveRaw(in: String, inLocation: String): Unit = {
    val localFileName = Random.alphanumeric.filter(_.isLetter).take(16).mkString

    try {
      reflect.io.File(localFileName).writeAll(in)
      val isExist: Boolean = minioClient.bucketExists(BucketExistsArgs.builder().bucket(configYaml.bucket).build())
      if !isExist then minioClient.makeBucket(MakeBucketArgs.builder().bucket(configYaml.bucket).build())

      minioClient.uploadObject(UploadObjectArgs.builder().`object`(localFileName).bucket(configYaml.bucket).filename(localFileName).build())
      copyFile(configYaml.bucket, localFileName, configYaml.bucket, inLocation)
      removeFile(configYaml.bucket, localFileName)

    } finally {
      new File(localFileName).delete()
    }
  }

  def deleteFolder(sourceSchema: String, folderName: String): Boolean = {
    val list: List[String] = getListElementsInFolder(sourceSchema, folderName)

    list.foreach(fileFullLocation =>
      removeFile(sourceSchema, fileFullLocation)
    )
    return true
  }

  private def getListElementsInFolder(sourceSchema: String, oldTable: String): List[String] = {
    var list: List[String] = List.apply()
    val listArgs = ListObjectsArgs.builder()
      .bucket(sourceSchema)
      .prefix(oldTable)
      .recursive(true)
      .build()

    val objects = minioClient.listObjects(listArgs)
    objects.forEach(i => list = list.appended(i.get().objectName()))
    return list
  }

  def getFolder(location: String): List[String] = {
    val listArgs = ListObjectsArgs.builder().bucket(configYaml.bucket).prefix(location).recursive(true).delimiter("/").build()

    var list: List[String] = List.apply()
    val objects = minioClient.listObjects(listArgs).asScala
    objects.foreach(result => {
      list = list.appended(result.get().objectName().replace(location + "/", "").split("/").init.mkString("/"))
    })

    return list.distinct
  }

  private def copyFile(bucketFrom: String, fileFrom: String, bucketTo: String, fileTo: String): Unit = {
    val copySource = CopySource.builder().bucket(bucketFrom).`object`(fileFrom).build()
    val copyArgs = CopyObjectArgs.builder().source(copySource).bucket(configYaml.bucket).`object`(fileTo).build()
    minioClient.copyObject(copyArgs)
  }

  private def removeFile(sourceSchema: String, fileFullLocation: String): Unit = {
    val removeArgs = RemoveObjectArgs.builder().bucket(sourceSchema).`object`(fileFullLocation).build()
    minioClient.removeObject(removeArgs)
  }

  override def moveTablePartition(sourceSchema: String, oldTable: String, newTableSchema: String, newTable: String, partitionName: List[String]): Boolean = {
    val list: List[String] = getListElementsInFolder(sourceSchema, oldTable)

    list.foreach(fileFullLocation =>
      copyFile(sourceSchema, fileFullLocation, sourceSchema, newTable + fileFullLocation.stripPrefix(oldTable))
      removeFile(sourceSchema, fileFullLocation)
    )
    return true
  }

  def modifySparkParameter(key: String, value: Option[String]): Unit = {
    if value.nonEmpty then
      SparkObject.setHadoopConfiguration(key, value.get)
  }

  def modifySpark(): Unit = {
    // Basic connection configuration
    SparkObject.setHadoopConfiguration("fs.s3a.endpoint", getMinIoHost)
    SparkObject.setHadoopConfiguration("fs.s3a.access.key", configYaml.accessKey)
    SparkObject.setHadoopConfiguration("fs.s3a.secret.key", configYaml.secretKey)
    SparkObject.setHadoopConfiguration("fs.s3a.establish.timeout", configYaml.establishTimeout.getOrElse("15000"))
    SparkObject.setHadoopConfiguration("fs.s3a.path.style.access", configYaml.pathStyleAccess.getOrElse(true).toString)

    // Configuration for large file handling and multipart uploads
    modifySparkParameter("fs.s3a.multipart.size", configYaml.multipartSize)
    modifySparkParameter("fs.s3a.multipart.threshold", configYaml.multipartThreshold)
    modifySparkParameter("fs.s3a.block.size", configYaml.blockSize)
    modifySparkParameter("fs.s3a.buffer.dir", configYaml.bufferDir)
    modifySparkParameter("fs.s3a.fast.upload", configYaml.fastUpload)
    modifySparkParameter("fs.s3a.fast.upload.buffer", configYaml.fastUploadBuffer)
    modifySparkParameter("fs.s3a.fast.upload.active.blocks", configYaml.fastUploadActiveBlocks)
    modifySparkParameter("fs.s3a.connection.maximum", configYaml.maxConnections)
    modifySparkParameter("fs.s3a.threads.max", configYaml.maxThreads)
    modifySparkParameter("fs.s3a.connection.timeout", configYaml.connectionTimeout)
    modifySparkParameter("fs.s3a.socket.recv.buffer", configYaml.socketReceiveBuffer)
    modifySparkParameter("fs.s3a.socket.send.buffer", configYaml.socketSendBuffer)

    // Retry configuration for failed operations
    modifySparkParameter("fs.s3a.retry.limit", configYaml.retryLimit)
    modifySparkParameter("fs.s3a.retry.interval", configYaml.retryInterval)
    modifySparkParameter("fs.s3a.attempts.maximum", configYaml.attemptsMaximum)

    // Additional configuration to handle uploads with MinIO compatibility
    modifySparkParameter("fs.s3a.upload.active.blocks", configYaml.uploadActiveBlocks)
    //    modifySparkParameter("fs.s3a.committer.name", configYaml.committerName)
    //    modifySparkParameter("fs.s3a.committer.staging.conflict-mode", configYaml.committerStagingConflictMode)
    //    modifySparkParameter("fs.s3a.committer.staging.tmp.path", configYaml.committerStagingTmpPath)
    //    modifySparkParameter("fs.s3a.committer.staging.unique-filenames", configYaml.committerStagingUniqueFilenames)

    // Additional timeout configurations from minio.yaml
    modifySparkParameter("fs.s3a.api.call.timeout", configYaml.apiCallTimeout)
    modifySparkParameter("fs.s3a.request.timeout", configYaml.requestTimeout)
    modifySparkParameter("fs.s3a.client.execution.timeout", configYaml.clientExecutionTimeout)

    // Temporary file handling
    modifySparkParameter("fs.s3a.enable.cleanup.temporary.files", configYaml.enableCleanupTemporaryFiles)
    modifySparkParameter("fs.s3a.temporary.file.cleanup.interval", configYaml.temporaryFileCleanupInterval)
    modifySparkParameter("fs.s3a.disable.temporary.files", configYaml.disableTemporaryFiles)

    // SSL configuration
    //    modifySparkParameter("fs.s3a.connection.ssl.enabled", configYaml.sslEnabled.getOrElse(false).toString)
    //    modifySparkParameter("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

  }

  private def getMinIoHost: String = {
    configYaml.minioHost.foreach(i => {
      val socket = new Socket()
      try {
        socket.connect(new java.net.InetSocketAddress(i.hostName, i.hostPort),
          configYaml.establishTimeout.getOrElse("30000").toInt)
        return i.getUrl
      } catch {
        case e: Exception =>
          false
      } finally {
        if (socket != null) socket.close()
      }
    })
    throw NotImplementedException("Connection check not implemented for MinIO")
  }

  def getLocation(location: String): String = {
    return s"s3a://${configYaml.bucket}/$location"
  }

  private def getLocationWithPostfix(location: String, keyPartitions: List[String], valuePartitions: List[String]): String = {
    var postfix: String = ""
    keyPartitions.zipWithIndex.foreach { case (value, index) => postfix += s"${keyPartitions(index)}=${valuePartitions(index)}/"
    }
    val location1 = s"$location/${postfix}"
    return location1
  }

  def getLocation(location: String, keyPartitions: List[String], valuePartitions: List[String]): String = {
    val location1 = s"s3a://${configYaml.bucket}/${getLocationWithPostfix(location, keyPartitions, valuePartitions)}"
    return location1
  }

  def getMasterFolder: String = configYaml.bucket

  override def close(): Unit = {
    ConnectionTrait.removeFromCache(getCacheKey())
  }

  override def getConnectionEnum(): ConnectionEnum = {
    ConnectionEnum.minioParquet
  }

  override def getConfigLocation(): String = {
    _configLocation
  }

  override def readDfSchema(location: String): DataFrame = {
    throw NotImplementedException("readDfSchema method not implemented for MinIO")
  }

  override def deleteFolder(location: String): Boolean = {
    val list = getListElementsInFolder(configYaml.bucket, location)
    list.foreach(fileFullLocation =>
      removeFile(configYaml.bucket, fileFullLocation)
    )
    return true
  }

  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit = {
    throw NotImplementedException("writeDfPartitionAuto method not implemented for MinIO")
  }


}
