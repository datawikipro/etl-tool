package pro.datawiki.sparkLoader.connection.jsonApi

import pro.datawiki.datawarehouse.DataFrameTrait

case class YamlConfig(
                  url: String,
                  schemas: List[YamlConfigSchema] = List.apply(),
                  apiToken: String,
                  cookies: List[KeyValue] = List.apply(),
                  authType: AuthType,
                  limit: Int,
                  startOffset: Int,
                  pageCount: Int
                ) {
  def getUrl: String = url

  def isValidationScript:Boolean={
    return schemas.nonEmpty
  }
  
  def getSchemaByJson(json: String):DataFrameTrait= {
    schemas.foreach(i=>{
      val result = i.getSchemaByJson(json)
      if result != null then return result
    })
    return null
  }
}