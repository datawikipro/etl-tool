package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.connection.{Connection, DatabaseTrait}

case class YamlConfigTransformationSaveLayerSTG(
                                              sourceName: String,
                                              sql: String
                                            ) extends YamlConfigTransformationSaveLayerTrait {
  
}