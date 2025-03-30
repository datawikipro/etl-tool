package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.SparkObject.spark
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait

case class YamlConfigTransformationSparkSql(
                                             sql: String
                                           ) extends YamlConfigTransformationTrait {
  override def getDataFrame: DataFrame = {
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    val df: DataFrame = SparkObject.spark.sql(sql)
    LogMode.debugDF(df)
    return df
  }

}
