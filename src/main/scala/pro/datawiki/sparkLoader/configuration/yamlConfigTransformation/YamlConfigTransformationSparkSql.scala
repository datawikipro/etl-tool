package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait

case class YamlConfigTransformationSparkSql(
                                             sql: String
                                           ) extends YamlConfigTransformationTrait {
  override def getDataFrame(): DataFrame = {
    val df: DataFrame = SparkObject.spark.sql(sql)
    df.show()
    return df
  }

}
