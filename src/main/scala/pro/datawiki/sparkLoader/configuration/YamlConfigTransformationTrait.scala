package pro.datawiki.sparkLoader.configuration

import org.apache.spark.sql.DataFrame

trait YamlConfigTransformationTrait {
  def getDataFrame:DataFrame
}
