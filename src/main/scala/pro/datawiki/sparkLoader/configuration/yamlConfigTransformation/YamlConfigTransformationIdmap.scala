package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.{LogMode, SparkObject}
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap.YamlConfigTransformationIdmapTemplate
import pro.datawiki.sparkLoader.connection.Connection
import pro.datawiki.sparkLoader.transformation.TransformationIdMap

case class YamlConfigTransformationIdmap(
                                          sourceName: String,
                                          connection: String,
                                          idmaps: List[YamlConfigTransformationIdmapTemplate]
                                        ) extends YamlConfigTransformationTrait {
  override def getDataFrame: DataFrame = {
    val connect= Connection.getConnection(connection)
    var df = SparkObject.spark.sql(s"select * from ${sourceName}")
    if LogMode.isDebug then {
      df.printSchema()
      df.show()
    }
    idmaps.foreach(j => {
      df = j.addendNewKeys(df,connect)
      if LogMode.isDebug then {
        df.printSchema()
        df.show()
      }
    })
    return df
  }

}