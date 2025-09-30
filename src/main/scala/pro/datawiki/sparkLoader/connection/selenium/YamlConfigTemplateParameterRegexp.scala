package pro.datawiki.sparkLoader.connection.selenium

import pro.datawiki.exception.ValidationException

import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

case class YamlConfigTemplateParameterRegexp(
                                              pattern: String,
                                              parameters: List[String]
                                            ) {
  def getResult(value: String): Map[String, SeleniumType] = {
    var lst: Map[String, SeleniumType] = Map()

    val pattern1: Regex = s"${pattern}".r
    if !pattern1.matches(value) then {
      throw ValidationException(s"Value '$value' does not match pattern '$pattern'")
    }
    val result = pattern1.findAllMatchIn(value).toList
    if result.length != 1 then throw ValidationException(s"Pattern '$pattern' matched ${result.length} times, expected exactly 1")
    if result.head.groupCount != parameters.length then throw ValidationException(s"Pattern '$pattern' has ${result.head.groupCount} groups, expected ${parameters.length}")

    for (a <- parameters.indices) {
      lst += (parameters(a) -> SeleniumString(result.head.group(a + 1)))
    }

    return lst
  }

  def getModified(in: Map[String, String]): YamlConfigTemplateParameterRegexp = {
    var newParameters: List[String] = List.apply()

    parameters.foreach(i => {
      newParameters = newParameters.appended(YamlConfig.getModifiedString(i, in))
    })

    return YamlConfigTemplateParameterRegexp(
      pattern = YamlConfig.getModifiedString(pattern, in),
      parameters = newParameters
    )
  }

}
