package pro.datawiki.sparkLoader.connection.minIo.minioBase

import com.typesafe.scalalogging.LazyLogging
import io.minio.{BucketExistsArgs, CopyObjectArgs, CopySource, ListObjectsArgs, MakeBucketArgs, MinioClient, RemoveObjectArgs, UploadObjectArgs}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, WriteMode}

import java.io.File
import java.net.{InetSocketAddress, Socket}

class LoaderMinIo(configYaml: YamlConfig) extends ConnectionTrait, LazyLogging {

  val minioClient: MinioClient = MinioClient.builder()
    .endpoint(getMinIoHost)
    .credentials(getAccessKey, getSecretKey)
    .build()

  def saveRaw(in: String, inLocation: String): Unit = {
    val locationList = inLocation.split(".".toCharArray.head)
    val location = locationList.mkString("/")
    try {
      reflect.io.File(inLocation).writeAll(in)
      val isExist: Boolean = minioClient.bucketExists(BucketExistsArgs.builder().bucket(configYaml.bucket).build())
      if (!isExist) {
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(configYaml.bucket).build())
      }

      minioClient.uploadObject(UploadObjectArgs.builder().`object`(location).bucket(configYaml.bucket).filename(inLocation).build())
    } finally {
      new File(location).delete()
    }
  }

  def moveTablePartition(sourceSchema: String, oldTable: String, newTable: String, partitionName: List[String], writeMode: WriteMode): Boolean = {

    // Список объектов в исходной папке
    val listArgs = ListObjectsArgs.builder()
      .bucket(sourceSchema)
      .prefix(oldTable)
      .recursive(true)
      .build()

    val objects = minioClient.listObjects(listArgs)
    objects.forEach { result =>
      val sourceObject = result.get().objectName()
      val destinationObject = newTable + sourceObject.stripPrefix(oldTable)
      val copySource = CopySource.builder().bucket(sourceSchema).`object`(sourceObject).build()
      val copyArgs = CopyObjectArgs.builder().source(copySource).bucket(configYaml.bucket).`object`(destinationObject).build()
      minioClient.copyObject(copyArgs)
      val removeArgs = RemoveObjectArgs.builder().bucket(sourceSchema).`object`(sourceObject).build()
      minioClient.removeObject(removeArgs)
    }
    return true
  }

  def modifySpark(): Unit = {
    val connectionTimeOut = "600000"
    SparkObject.setHadoopConfiguration("fs.s3a.endpoint", getMinIoHost)
    SparkObject.setHadoopConfiguration("fs.s3a.access.key", getAccessKey)
    SparkObject.setHadoopConfiguration("fs.s3a.secret.key", getSecretKey)
    SparkObject.setHadoopConfiguration("fs.s3a.establish.timeout", "5000")
    SparkObject.setHadoopConfiguration("fs.s3a.path.style.access", "true")
  }

  val timeout: Int = 5000

  private def getMinIoHost: String = {
    configYaml.minioHost.foreach(i => {
      val socket = new Socket()
      try {
        socket.connect(new java.net.InetSocketAddress(i.hostName, i.hostPort), timeout)
        return i.getUrl
      } catch {
        case e: Exception =>
          false
      } finally {
        if (socket != null) socket.close()
      }
    })
    throw Exception()
  }

  private def getAccessKey: String = configYaml.accessKey

  private def getSecretKey: String = configYaml.secretKey

}

