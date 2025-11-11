package pro.datawiki.sparkLoader.connection.minIo.minioBase

import _root_.org.apache.spark.sql.functions.lit
import _root_.org.apache.spark.sql.{DataFrame, DataFrameReader}
import io.minio.*
import pro.datawiki.exception.{NotImplementedException, TableNotExistException}
import pro.datawiki.sparkLoader.connection.fileBased.{FileBaseFormat, FileStorageCommon}
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

  def optimizeDataFramePartitions(df: DataFrame): DataFrame = FileStorageCommon.optimizeDataFramePartitions(df)

  private def createDataFrameReader(): DataFrameReader =
    FileStorageCommon.createDataFrameReader(
      format,
      configYaml.corruptRecordColumn,
      configYaml.jsonMode,
      configYaml.jsonMultiline
    )

  def readDf(location: String): DataFrame = {
    val fullLocation = getLocation(location = location)
    logInfo(s"Reading DataFrame from MinIO location: $fullLocation")

    val df: DataFrame = createDataFrameReader().load(fullLocation)
    logInfo(s"Successfully read DataFrame from: $fullLocation")
    return df

  }

  def readDf(location: String, keyPartitions: List[String], valuePartitions: List[String], withPartitionOnDataframe: Boolean): DataFrame = {
    val fullLocation = getLocation(location = location, keyPartitions = keyPartitions, valuePartitions = valuePartitions)
    val partitionPath = getLocationWithPostfix(location, keyPartitions, valuePartitions)

    logInfo(s"Reading DataFrame from MinIO with partitions: $fullLocation")
    logInfo(s"Partition path: $partitionPath, keyPartitions: ${keyPartitions.mkString(",")}, valuePartitions: ${valuePartitions.mkString(",")}")
    logInfo(s"Using format: ${format.toString} for reading")

    val list = getListElementsInFolder(partitionPath)
    if list.isEmpty then {
      logWarning(s"No files found in partition path: $partitionPath")
      throw TableNotExistException(s"No files found in MinIO partition path: $partitionPath")
    }

    logInfo(s"Found ${list.size} files in partition: ${list.take(5).mkString(", ")}${if (list.size > 5) "..." else ""}")


    var df: DataFrame = createDataFrameReader().load(fullLocation)
    if withPartitionOnDataframe then {
      keyPartitions.zipWithIndex.foreach { case (value, index) =>
        df = df.withColumn(keyPartitions(index), lit(valuePartitions(index)))
      }
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
      removeFile(localFileName)

    } finally {
      new File(localFileName).delete()
    }
  }

  override def deleteFolder(folderName: String): Boolean = {
    val list: List[String] = getListElementsInFolder(folderName)

    list.foreach(fileFullLocation =>
      removeFile(fileFullLocation)
    )
    return true

  }

  private def getListElementsInFolder(oldTable: String): List[String] = {
    var list: List[String] = List.apply()
    val listArgs = ListObjectsArgs.builder()
      .bucket(configYaml.bucket)
      .prefix(oldTable)
      .recursive(true)
      .build()

    val objects = minioClient.listObjects(listArgs)
    objects.forEach(i => list = list.appended(i.get().objectName()))
    return list
  }

  def getFolder(location: String): List[String] = {
    val listArgs = ListObjectsArgs.builder().bucket(configYaml.bucket).prefix(location).delimiter("/").recursive(true).build()
    val objects = minioClient.listObjects(listArgs).asScala
    val list: List[String] = objects.map(col => col.get().objectName()).
      filterNot(col => col.contains("_spark_metadata")).
      filterNot(col => col.contains("_temporary")).
      toList.distinct

    return list
  }

  private def copyFile(bucketFrom: String, fileFrom: String, bucketTo: String, fileTo: String): Unit = {
    val copySource = CopySource.builder().bucket(bucketFrom).`object`(fileFrom).build()
    val copyArgs = CopyObjectArgs.builder().source(copySource).bucket(configYaml.bucket).`object`(fileTo).build()
    minioClient.copyObject(copyArgs)
  }

  private def removeFile(fileFullLocation: String): Unit = {
    val removeArgs = RemoveObjectArgs.builder().bucket(configYaml.bucket).`object`(fileFullLocation).build()
    minioClient.removeObject(removeArgs)
  }

  override def moveTablePartition(oldTable: String, newTable: String, partitionName: List[String]): Boolean = {
    if partitionName.isEmpty then throw Exception()
    partitionName.foreach(col => {
      val list: List[String] = getListElementsInFolder(s"$oldTable/$col")

      list.foreach(fileFullLocation =>
        copyFile(configYaml.bucket, fileFullLocation, configYaml.bucket, newTable + fileFullLocation.stripPrefix(oldTable))
        removeFile(fileFullLocation)
      )
    }
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

  private def getLocationWithPostfix(location: String, keyPartitions: List[String], valuePartitions: List[String]): String =
    FileStorageCommon.buildPartitionedLocation(location, keyPartitions, valuePartitions)

  def getLocation(location: String, keyPartitions: List[String], valuePartitions: List[String]): String =
    FileStorageCommon.s3aUri(configYaml.bucket, getLocationWithPostfix(location, keyPartitions, valuePartitions))

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

  override def writeDfPartitionAuto(df: DataFrame, location: String, partitionName: List[String], writeMode: WriteMode): Unit = {
    throw NotImplementedException("writeDfPartitionAuto method not implemented for MinIO")
  }


}
