package pro.datawiki.sparkLoader.configuration

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

object RunConfig {
  private var partition: String = ""
  private var subPartition: String = ""

  def setPartition(in: String,inSubpartition: String): Unit = {
    partition = URLEncoder.encode(in, StandardCharsets.UTF_8.toString)

    subPartition = URLEncoder.encode(inSubpartition, StandardCharsets.UTF_8.toString)
    if subPartition == ""
      then subPartition = null

  }

  def getPartition: String = {
    partition
  }
//run_id=manual__2025-03-08T18%253A42%253A35.220191%252B00%253A00
      //'manual__2025-03-08T18%3A42%3A35.220191%2B00%3A00'
  def getSubPartition: String = {
    subPartition
  }

}
