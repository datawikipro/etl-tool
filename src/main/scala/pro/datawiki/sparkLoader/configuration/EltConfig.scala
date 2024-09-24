package pro.datawiki.sparkLoader.configuration

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.nio.file.{Files, Paths}

case class EltConfig(
                      connections: List[YamlConfigSources],
                      source: List[YamlConfigSource],
                      idmap: YamlConfigIdmap,
                      transformations: List[YamlConfigTransformation],
                      target: YamlConfigTarget
                    )

object EltConfig {
  def apply(inConfig:String): EltConfig = {
    val lines = Files.readString(Paths.get(inConfig))
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val result = mapper.readValue(lines, classOf[EltConfig])
    return result
  }
}
