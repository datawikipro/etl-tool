package pro.datawiki.sparkLoader.connection.jsonApi

import pro.datawiki.datawarehouse.DataFrameTrait

case class YamlConfig(
                       url: String,
                       schemas: List[YamlConfigSchema] = List.apply(),
                       method: String = "Get",
                       cookies: List[KeyValue] = List.apply(),
                       authType: AuthType,
                       body: String,
                       resultType:String = "json",
                       host: String,
                       limit: Int,
                       startOffset: Int,
                       pageCount: String
                     ) {
  def getUrl: String = url
  def getHost:String = {
    return host
  }
  def isValidationScript: Boolean = {
    return schemas.nonEmpty
  }

  def getSchemaByJson(json: String): DataFrameTrait = {
    schemas.foreach(i => {
      val result = i.getSchemaByJson(json)
      if result != null then return result
    })
    return null
  }
}