package pro.datawiki.sparkLoader.connection.jsonApi

case class YamlConfig(
                       url: String = throw IllegalArgumentException("URL is required"),
                       method: String = "Get",
                       cookies: List[KeyValue] = List.apply(),
                       headers: List[KeyValue] = List.apply(),
                       authType: AuthType,
                       body: String,
                       resultType: String = "json",
//                       host: String = throw IllegalArgumentException("host is required"),
                       limit: Int,
                       startOffset: Int,
                       pageCount: String,
                       threadCount: Int = 5,
                       timeoutSeconds: Int = 10
                     ) {
  
}