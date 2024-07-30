package source.mysql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.nio.file.{Files, Paths}

import java.util.Properties

class LoaderMySql(inConfig:String)  {
  var configYaml: YamlConfig = null
  val lines: String = Files.readString(Paths.get(inConfig))

  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)
  configYaml = mapper.readValue(lines, classOf[YamlConfig])
  
  def getProperties:Properties = {
    val prop = new java.util.Properties
    prop.setProperty("user", configYaml.user)
    prop.setProperty("password", configYaml.password)
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    return prop
  }
  
  def getJdbc:String = {
    return s"jdbc:mysql://${configYaml.host}:3306"
  }
}
