package pro.datawiki.sparkLoader.connection.minIo.minioBase

import com.typesafe.scalalogging.LazyLogging
import io.minio.*
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.{ConnectionTrait, WriteMode}

import java.io.File
import java.net.URLEncoder.*
import java.net.{InetSocketAddress, Socket}
import scala.jdk.CollectionConverters.*
import scala.util.Random

case class LoaderMinIo(configYaml: YamlConfig, timeout: Int = 15 * 1000) extends ConnectionTrait, LazyLogging {

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
  
  def deleteFolder(sourceSchema: String, folderName: String):Boolean = {
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
    objects.forEach(i => list = list.appended(i.get().objectName())
    )
    return list
  }

  def getFolder(sourceSchema: String, location: String): List[String] = {
    val listArgs = ListObjectsArgs.builder().bucket(sourceSchema).prefix(location).recursive(true).delimiter("/").build()

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

  def moveTablePartition(sourceSchema: String, oldTable: String, newTable: String, partitionName: List[String], writeMode: WriteMode): Boolean = {
    val list: List[String] = getListElementsInFolder(sourceSchema, oldTable)

    list.foreach(fileFullLocation =>
      copyFile(sourceSchema, fileFullLocation, sourceSchema, newTable + fileFullLocation.stripPrefix(oldTable))
      removeFile(sourceSchema, fileFullLocation)
    )
    return true
  }

  def modifySpark(): Unit = {
    val connectionTimeOut = "600000"
    SparkObject.setHadoopConfiguration("fs.s3a.endpoint", getMinIoHost)
    SparkObject.setHadoopConfiguration("fs.s3a.access.key", configYaml.accessKey)
    SparkObject.setHadoopConfiguration("fs.s3a.secret.key", configYaml.secretKey)
    SparkObject.setHadoopConfiguration("fs.s3a.establish.timeout", s"${timeout}")
    SparkObject.setHadoopConfiguration("fs.s3a.path.style.access", "true")
  }

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

  def getLocation(location: String): String = {
    return s"s3a://${configYaml.bucket}/$location"
  }

  def getLocationWithPostfix(location: String, keyPartitions: List[String], valuePartitions: List[String]): String = {
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
  
  override def close(): Unit = {}
}
