package pro.datawiki.sparkLoader.configuration

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object RunConfig {
  private var partition: String = ""
  private var subPartition: String = ""

  def setPartition(inPartition: String, inSubPartition: String): Unit = {
    val md = MessageDigest.getInstance("SHA-256")
    val sha= md.digest(inPartition.getBytes("UTF-8")).map("%02x".format(_)).mkString
    var dt:String = ""
    try {
      dt = inPartition.substring(11, 21)
    } catch
      case _=> dt = "unknown"
    partition= s"""${dt}_${sha}"""

    if partition == "" then partition = null
    
    subPartition = inSubPartition
    if subPartition == "" then subPartition = null

  }

  def getPartition: String = {
    partition
  }

  def getSubPartition: String = {
    subPartition
  }

}
