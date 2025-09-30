package pro.datawiki.textVariable

import pro.datawiki.exception.DataProcessingException

import scala.collection.mutable

object WorkWithText {

  def replaceWithoutDecode(in: String, row: mutable.Map[String, String]): String = {
    var result: String = in
    row.foreach(i => {
      try {
        result = result.replace(s"""$${${i._1}}""", i._2)
      } catch {
        case _ => {
          throw DataProcessingException(s"Error processing text replacement for variable: ${i._1}")
        }
      }
    })

    return result
  }
}
