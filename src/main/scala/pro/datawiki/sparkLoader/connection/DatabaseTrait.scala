package pro.datawiki.sparkLoader.connection

import org.apache.spark.sql.DataFrame

trait DatabaseTrait {
  def getDataFrameBySQL(sql:String): DataFrame// = {throw Exception()}
  def createTempTable(df:DataFrame):Unit = {throw Exception()}
  def insertCCdToIdmap(df:DataFrame,domainName:String,tenantName:String,
                       hasRk: Boolean):Unit// = {throw Exception()}
  def getIdmapDataFrame(domainName:String, tenantName:String):DataFrame// ={throw Exception()}
}
