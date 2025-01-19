package pro.datawiki.sparkLoader.connection.selenium

import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

case class YamlConfigTemplateParameterRegexp(
                                              pattern: String,
                                              parameters: List[String]
                                            ) {
  def getResultParse(value: String): List[KeyValue] = {
    var lst: List[KeyValue] = List.apply()

    val pattern1: Regex = s"${pattern}".r
    if !pattern1.matches(value) then {
      throw Exception()
    }
    val result = pattern1.findAllMatchIn(value).toList
    if result.length != 1 then throw Exception()
    if result.head.groupCount != parameters.length then throw Exception()

    for (a <- parameters.indices) {
      lst = lst :+ KeyValue(parameters(a), result.head.group(a + 1))
    }

    return lst
  }
}
