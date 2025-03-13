package pro.datawiki.sparkLoader.configuration.yamlConfigTransformation.yamlConfigTransformationIdmap

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws}
import pro.datawiki.sparkLoader.connection.ConnectionTrait
import pro.datawiki.sparkLoader.transformation.{TransformationCache, TransformationCacheTrait, TransformationIdMap}

case class YamlConfigTransformationIdmapTemplate(
                                                  domainName: String,
                                                  isGenerated: Boolean,
                                                  rkKey: String,
                                                  systemCode: String,
                                                  columnNames: List[String]
                                                ){
  
  def addendNewKeys(df: DataFrame, connection: ConnectionTrait): DataFrame = {
//    val cache: TransformationCacheTrait = TransformationCache.appluByaConnection(connection)
    
    var existsKeys = TransformationIdMap.connect.getIdmapDataFrame(domainName, systemCode)
    val ccdKey = concat_ws(TransformationIdMap.constantSeparator, columnNames.map(col) *).as("ccd")
    val keys = df.
      select(ccdKey).
      where("""ccd is not null and ccd != """"").
      join(existsKeys, "ccd", "left").
      where("rk is null").
      distinct()

    TransformationIdMap.connect.insertCCdToIdmap(keys, domainName, systemCode, false)
    if isGenerated then {
      //throw Exception()
    }
    existsKeys = TransformationIdMap.connect.getIdmapDataFrame(domainName, systemCode)

    var df2 = df
    if !(columnNames.length == 1 && columnNames.head == "ccd") then {
      df2 = df2.withColumn("ccd", ccdKey)
    }
    df2 = df2.join(existsKeys, "ccd", "left").select(df.col("*"), existsKeys.col("rk").as(rkKey))

    return df2
  }

  def addNewKeys(df: DataFrame): Unit = {
    TransformationIdMap.connect.insertCCdToIdmap(df, domainName, systemCode, true)
  }

  def getCCDByRk: DataFrame = {
    return TransformationIdMap.connect.getIdmapDataFrame(domainName, systemCode)
  }
}
