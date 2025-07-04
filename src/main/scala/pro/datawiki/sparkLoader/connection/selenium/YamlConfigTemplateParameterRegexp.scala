package pro.datawiki.sparkLoader.connection.selenium

import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

case class YamlConfigTemplateParameterRegexp(
                                              pattern: String,
                                              parameters: List[String]
                                            ){
  def getResult(value: String): Map[String, SeleniumType] = {
    var lst: Map[String, SeleniumType] = Map()

    val pattern1: Regex = s"${pattern}".r
    if !pattern1.matches(value) then {
      throw Exception()
    }
    val result = pattern1.findAllMatchIn(value).toList
    if result.length != 1 then throw Exception()
    if result.head.groupCount != parameters.length then throw Exception()

    for (a <- parameters.indices) {
      lst += (parameters(a)->SeleniumString(result.head.group(a + 1)))
    }

    return lst
  }

  def getModified(in: mutable.Map[String, String]): YamlConfigTemplateParameterRegexp = {
    var newParameters: List[String] = List.apply()
    
    parameters.foreach(i=> {
      newParameters = newParameters.appended(YamlConfig.getModifiedString(i, in))
    })
    
    return YamlConfigTemplateParameterRegexp(
      pattern = YamlConfig.getModifiedString(pattern, in),
      parameters= newParameters
    )
  }
  
}
