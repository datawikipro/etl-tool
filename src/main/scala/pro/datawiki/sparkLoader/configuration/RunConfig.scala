package pro.datawiki.sparkLoader.configuration

import java.security.MessageDigest
import scala.util.matching.Regex

object RunConfig {
  private var partition: String = ""
  private var subPartition: String = ""

  def getPartition(in: String): String = {
    val patterns: List[Regex] = List.apply(
      """^scheduled__(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\+(\d{2}):(\d{2})$""".r,
      """^manual__(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{6})\+(\d{2}):(\d{2})$""".r
    )
    patterns.foreach(pattern => {
      if pattern.matches(in) then {
        for (patternMatch <- pattern.findAllMatchIn(in)) {
          return s"""${patternMatch.group(1)}-${patternMatch.group(2)}-${patternMatch.group(3)} ${patternMatch.group(4)}_${patternMatch.group(5)}_${patternMatch.group(6)}"""
        }
      }
    })

    val md = MessageDigest.getInstance("SHA-256")
    return md.digest(in.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }

  def initPartitions(inPartition: String, inSubPartition: String): Unit = {
    partition = getPartition(inPartition)

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
