package pro.datawiki.sparkLoader.configuration

import org.apache.spark.sql.DataFrame

trait YamlConfigSourceTrait {
  def getDataFrame(sourceName:String):DataFrame = {throw Exception()}
}
