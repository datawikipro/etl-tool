package pro.datawiki.sparkLoader.configuration

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.nio.file.{Files, Paths}
import pro.datawiki.sparkLoader.YamlClass

case class EltConfig(
                      connections: List[YamlConfigConnections],
                      source: List[YamlConfigSource],
                      idmap: YamlConfigIdmap,
                      transformations: List[YamlConfigTransformation],
                      target: YamlConfigTarget
                    ){
  def getSource:List[YamlConfigSource] = {
    if source ==null then return List.apply()
    return source
  }
}

object EltConfig extends YamlClass{
  def apply(inConfig:String): EltConfig = {
    val result = mapper.readValue(getLines(inConfig), classOf[EltConfig])
    return result
  }
}
