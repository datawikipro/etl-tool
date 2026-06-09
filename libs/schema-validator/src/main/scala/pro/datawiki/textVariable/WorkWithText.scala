package pro.datawiki.textVariable

import pro.datawiki.exception.DataProcessingException

import java.net.URLEncoder

object WorkWithText {

  def replaceWithoutDecode(in: String, row: Map[String, String], mode: String = "raw"): String = {
    var result: String = in
    row.foreach(i => {
      try {

        result = result.replace(s"""$${${i._1}}""", mode match {
          case "raw" => i._2
          case "urlEncode" => URLEncoder.encode(i._2, "UTF-8")
          case _ => throw Exception()
        })

      } catch {
        case _ => {
          throw DataProcessingException(s"Error processing text replacement for variable: ${i._1}")
        }
      }
    })

    return result
  }


}
