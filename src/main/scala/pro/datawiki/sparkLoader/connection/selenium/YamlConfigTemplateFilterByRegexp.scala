package pro.datawiki.sparkLoader.connection.selenium

import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

class YamlConfigTemplateFilterByRegexp(
                                varName: String,
                                regexp: String
                              ) {

  def checkRegexp(in: Map[String, SeleniumType]): Boolean = {
    in.foreach(i =>
      if i._1 == varName then {
        val pattern1: Regex = s"${regexp}".r
        val res = i._2 match
          case x: SeleniumString => x.getValue
          case _ => throw Exception()
        val tst = pattern1.matches(res)
        return tst
      }
    )
    return false
  }
  
}
