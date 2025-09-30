package pro.datawiki.sparkLoader.connection

trait SupportIdMap {
  def mergeIdMap(inTable: String, domain: String, inSystemCode: String, outSystemCode: String): Boolean

  def generateIdMap(inTable: String, domain: String, systemCode: String): Boolean

}
