package pro.datawiki.sparkLoader.connection.selenium

import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

class YamlConfigTemplateFilterByRegexp(
                                        varName: String,
                                        regexp: String
                                      ) {

  def checkRegexp(in: Map[String, SeleniumType]): Boolean = {
    in.get(varName) match {
      case Some(selType) =>
        val pattern1: Regex = regexp.r
        val res = selType match {
          case x: SeleniumString => x.getValue
          case _ => throw UnsupportedOperationException("Unsupported type for regexp filtering")
        }
        pattern1.matches(res)
      case None => false
    }
  }


  def getModified(parameters: Map[String, String]): YamlConfigTemplateFilterByRegexp = {
    return YamlConfigTemplateFilterByRegexp(
      varName = YamlConfig.getModifiedString(varName, parameters),
      regexp = YamlConfig.getModifiedString(regexp, parameters),
    )
  }
}
