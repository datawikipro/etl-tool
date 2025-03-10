package pro.datawiki.sparkLoader.connection.network

import com.typesafe.scalalogging.LazyLogging
import io.minio.{BucketExistsArgs, MakeBucketArgs, MinioClient, UploadObjectArgs}
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.ConnectionTrait

import java.io.File
import java.net.{InetSocketAddress, Socket}

class Connection(host:String, port:Int) {
  val timeout: Int = 5000
  
  def validateHost:Boolean ={
    val socket = new Socket()
    try {
      socket.connect(new java.net.InetSocketAddress(host, port), timeout)
      return true
    } catch {
      case e: Exception =>
        return false
    } finally {
      if (socket != null) socket.close()
    }
  }
}
