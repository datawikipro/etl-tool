package pro.datawiki.sparkLoader.configuration.yamlConfigSource

import org.apache.spark.sql.Row
import pro.datawiki.datawarehouse.DataFrameTrait
import pro.datawiki.sparkLoader.transformation.TransformationCacheTrait

trait YamlConfigSourceApiTrait {
  def getDataFrameAdHoc(sourceName: String,
                        adHoc: Row,
                        cache: TransformationCacheTrait): DataFrameTrait

  def getDataFrame(sourceName: String, cache: TransformationCacheTrait): DataFrameTrait
}
