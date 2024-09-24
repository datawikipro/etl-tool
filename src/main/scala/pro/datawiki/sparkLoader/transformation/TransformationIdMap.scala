package pro.datawiki.sparkLoader.transformation

import org.apache.spark.sql.DataFrame
import pro.datawiki.sparkLoader
import pro.datawiki.sparkLoader.SparkObject
import pro.datawiki.sparkLoader.connection.DatabaseTrait
import org.apache.spark.sql.functions.{col, concat_ws, lit}

import scala.jdk.CollectionConverters.*

class TransformationIdMap(sourceName: String,
                               tenantName: String,
                               domainName: String,
                               isGenerated: Boolean,
                               columnNames: List[String]) {
  def addendNewKeys(df: DataFrame): DataFrame = {
    var existsKeys = TransformationIdMap.connect.getIdmapDataFrame(domainName, tenantName)
    val ccdKey = concat_ws(TransformationIdMap.constantSeparator, columnNames.map(col): _*).as("ccd")
    val keys = df.select(ccdKey).where("""ccd is not null and ccd != """"").join(existsKeys, "ccd", "left").where("rk is null").distinct()
    
    TransformationIdMap.connect.insertCCdToIdmap(keys, domainName, tenantName)
    if isGenerated then {
      //throw Exception()
    }
    existsKeys = TransformationIdMap.connect.getIdmapDataFrame(domainName, tenantName)
    val df2 = df.withColumn("ccd", ccdKey).join(existsKeys, "ccd", "left").select(df.col("*"),existsKeys.col("rk").as(s"${domainName}_rk"))
    return df2
  }
}

object TransformationIdMap {
  def constantSeparator: String = "!@#"

  def apply(sourceName: String,
            tenantName: String,
            domainName: String,
            isGenerated: Boolean,
            columnNames: List[String]): TransformationIdMap = {
    return new TransformationIdMap(sourceName, tenantName, domainName,isGenerated, columnNames)
  }

  var connect: DatabaseTrait = null

  def setIdmap(in: DatabaseTrait): Unit = {
    connect = in
  }

  def initTable(domainName: String): Unit = {

  }

}