package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.configuration.YamlConfigTransformationTrait
import pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap.YamlConfigTransformationIdmapTemplate
import pro.datawiki.sparkLoader.transformation.TransformationIdMap

case class YamlConfigTransformationIdmap(
                                          sourceName: String,
                                          tenantName: String,
                                          idmaps: List[YamlConfigTransformationIdmapTemplate]
                                        ) extends YamlConfigTransformationTrait {
  override def getDataFrame: DataFrame = {
    var df = SparkObject.spark.sql(s"select * from ${sourceName}")
    df.show()
    idmaps.foreach(j => {
      val idmap = TransformationIdMap(
        domainName = j.domainName,
        rkKey = j.rkKey,
        systemCode = j.systemCode,
        isGenerated = j.isGenerated,
        columnNames = j.columnNames)
      df = idmap.addendNewKeys(df)
      df.show()
    })
    return df
  }

}