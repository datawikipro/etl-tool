package pro.datawiki.textVariable

import scala.collection.mutable

object WorkWithText {

  def replaceWithDecode():Unit={
    
  }

  def replaceWithoutDecode(in: String, row: mutable.Map[String, String]): String = {
    var result: String = in
    row.foreach(i => {
      result = result.replace(s"""$${${i._1}}""", i._2)
    })

    return result
  }
}
