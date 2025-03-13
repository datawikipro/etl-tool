package pro.datawiki.sparkLoader.connection.jsonApi

case class YamlConfig(
                  url: String,
                  schemas: List[YamlConfigSchema] = List.apply(),
                  apiToken: String,
                  cookies: List[KeyValue] = List.apply()
                ) {
  def getUrl: String = url

  def getSchemaByDataFrame(df: String):String = {
    if schemas.isEmpty then return null
    schemas.foreach(i=>{
      val result = i.getSchemaByDataFrame(df)
      if result != null then return result
    })
    return "error"
  }
}