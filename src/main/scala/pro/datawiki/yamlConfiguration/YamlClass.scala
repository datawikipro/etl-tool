package pro.datawiki.yamlConfiguration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.nio.file.{Files, Paths}

class YamlClass {
  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)
  //      .configure(com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, false)

  def getLines(inConfig: String): String = {
    val a = Files.readString(Paths.get(inConfig))
    return a
  }

  def toYaml(obj: Any): String =
    mapper.writeValueAsString(obj)
}
